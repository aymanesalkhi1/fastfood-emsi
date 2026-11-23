"""Partie 6 — pipeline de production : silver Parquet, or Delta, reprise par checkpoint."""

from __future__ import annotations

import os
import time

from pyspark.sql.functions import col

from .io import list_names, read_orders, read_restaurants, read_text
from .session import build_spark, stop
from .streaming import checkpoint, orders_stream, start_console, start_sink
from .transforms import (
    courier_sessions,
    enrich,
    sliding_cancel_rate,
    tumbling_kpi,
)

EXACTLY_ONCE = (
    "Exactly-once ici = source rejouable + checkpoint + sink transactionnel. "
    "Kafka conserve les messages ; le checkpoint mémorise le dernier offset "
    "validé en même temps que le commit du sink. Delta commite la version et "
    "l'offset de façon atomique. Au redémarrage, Spark reprend après le dernier "
    "offset commité : pas de trou, pas de seconde écriture du même micro-batch. "
    "Le sink console n'a pas de commit : un redémarrage réaffiche les lignes."
)


def start(spark, *, available_now: bool = False) -> list:
    restaurants = read_restaurants(spark).cache()
    restaurants.count()
    trigger = "10 seconds"
    queries = []

    silver = orders_stream(spark, restaurants)
    queries.append(
        start_sink(
            silver,
            name="silver-orders",
            fmt="parquet",
            path=lake_silver(),
            checkpoint=checkpoint("silver_orders"),
            partition_by=("city", "order_date"),
            trigger=trigger,
            available_now=available_now,
        )
    )

    delivered = orders_stream(spark, restaurants).filter(col("status") == "DELIVERED")
    kpi = tumbling_kpi(delivered)
    queries.append(
        start_sink(
            kpi,
            name="gold-kpi",
            fmt="delta",
            path=lake_gold(),
            checkpoint=checkpoint("gold_kpi"),
            trigger=trigger,
            available_now=available_now,
        )
    )
    if not available_now:
        # update montre les fenêtres ouvertes. append (Delta) n'écrit que les fenêtres closes.
        live = tumbling_kpi(orders_stream(spark, restaurants).filter(col("status") == "DELIVERED"))
        queries.append(start_console(live, "gold-live", mode="update", rows=20))

    sliding = sliding_cancel_rate(orders_stream(spark, restaurants))
    queries.append(
        start_sink(
            sliding,
            name="cancel-rate",
            fmt="parquet",
            path=lake_cancel(),
            checkpoint=checkpoint("cancel_rate"),
            trigger=trigger,
            available_now=available_now,
        )
    )
    sessions = courier_sessions(orders_stream(spark, restaurants))
    queries.append(
        start_sink(
            sessions,
            name="courier-sessions",
            fmt="parquet",
            path=lake_sessions(),
            checkpoint=checkpoint("courier_sessions"),
            trigger=trigger,
            available_now=available_now,
        )
    )
    return queries


def lake_silver() -> str:
    from .paths import lake

    return lake("silver", "orders_clean")


def lake_gold() -> str:
    from .paths import lake

    return lake("gold", "kpi_city_10min")


def lake_cancel() -> str:
    from .paths import lake

    return lake("gold", "cancel_rate_sliding")


def lake_sessions() -> str:
    from .paths import lake

    return lake("gold", "courier_sessions")


def gold_counts(spark, path: str | None = None) -> dict:
    """Compte les lignes de l'or et les couples (ville, fenêtre). Égaux : pas de doublon."""
    location = path or lake_gold()
    frame = spark.read.format("delta").load(location)
    total = frame.count()
    distinct = frame.select("city", "window_start").distinct().count()
    keys = {
        (row.city, str(row.window_start))
        for row in frame.select("city", "window_start").collect()
    }
    print(f"Delta {location} : count(*) = {total} , count(distinct city, window_start) = {distinct}")
    return {"path": location, "count": int(total), "distinct": int(distinct), "keys": keys}


def prove_restart(spark, root: str | None = None) -> dict:
    """Arrêt puis redémarrage du même checkpoint Delta.

    1. Premier lot commité. On note count(*).
    2. On relance sans nouveau fichier : le compte ne bouge pas, rien n'est réécrit.
    3. Le producteur ajoute un second lot. On relance. Les anciennes fenêtres sont
       toujours là, les nouvelles s'ajoutent, et count(*) reste égal au nombre
       de couples (ville, fenêtre).
    """
    import tempfile
    from pathlib import Path

    from .contract import TIMESTAMP_FORMAT
    from .generator import build_restaurants, generate_events, write_jsonl
    from .schemas import ORDER_SCHEMA
    from .transforms import deduplicate, tumbling_kpi

    base = Path(root) if root else Path(tempfile.mkdtemp(prefix="fastfood-reprise-"))
    source = base / "source"
    source.mkdir(parents=True, exist_ok=True)
    gold = str(base / "gold")
    checkpoint_dir = str(base / "checkpoint")

    events, _stats = generate_events(200, build_restaurants(), seed=42)
    events = sorted(events, key=lambda item: item["event_time"])
    cut = len(events) // 2
    write_jsonl(source / "part-a.json", events[:cut])

    def run_once(name: str) -> None:
        stream = (
            spark.readStream.schema(ORDER_SCHEMA)
            .option("timestampFormat", TIMESTAMP_FORMAT)
            .json(str(source))
        )
        query = start_sink(
            tumbling_kpi(deduplicate(stream)),
            name=name,
            fmt="delta",
            path=gold,
            checkpoint=checkpoint_dir,
            available_now=True,
        )
        query.awaitTermination()

    run_once("reprise-avant")
    before = gold_counts(spark, gold)
    print(f"Avant arrêt : {before['count']} lignes")

    run_once("reprise-meme-source")
    same = gold_counts(spark, gold)
    print(f"Après redémarrage, sans événement nouveau : {same['count']} lignes")

    write_jsonl(source / "part-b.json", events[cut:])
    run_once("reprise-apres")
    after = gold_counts(spark, gold)
    print(f"Après redémarrage, producteur continué : {after['count']} lignes")

    if before["count"] <= 0:
        raise RuntimeError("La table Delta Gold est vide après le premier commit.")
    if before["count"] != before["distinct"] or same["count"] != same["distinct"] or after["count"] != after["distinct"]:
        raise RuntimeError("Doublon : count(*) différent du nombre de couples (ville, fenêtre).")
    if same["count"] != before["count"] or same["keys"] != before["keys"]:
        raise RuntimeError("Le redémarrage a réécrit un micro-batch déjà commité.")
    if not before["keys"] <= after["keys"] or after["count"] <= before["count"]:
        raise RuntimeError("Perte : une fenêtre commitée avant l'arrêt a disparu.")

    result = {
        "avant": before["count"],
        "apres_redemarrage": same["count"],
        "apres_nouveaux_evenements": after["count"],
        "distinct_final": after["distinct"],
        "sans_perte": True,
        "sans_doublon": True,
    }
    print(
        "Reprise OK : "
        f"{result['avant']} avant, "
        f"{result['apres_redemarrage']} après redémarrage, "
        f"{result['apres_nouveaux_evenements']} une fois le producteur continué. "
        "count(*) = count(distinct city, window_start)."
    )
    return result


def describe_gold(spark) -> list[dict]:
    path = lake_gold()
    frame = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    rows = [row.asDict(recursive=True) for row in frame.collect()]
    frame.show(20, truncate=False)
    return rows


def print_offsets(spark, name: str = "gold_kpi") -> None:
    folder = checkpoint(name) + "/offsets"
    names = list_names(spark, folder)
    print(f"Checkpoint {folder} : {names}")
    numeric = [item for item in names if item.isdigit()]
    if not numeric:
        return
    latest = max(numeric, key=int)
    print(read_text(spark, f"{folder}/{latest}"))


def preview_batch(spark, limit: int = 12) -> list[dict]:
    """Même définition que l'or, calculée en batch sur l'historique pour la lecture du rapport."""
    restaurants = read_restaurants(spark)
    kpi = tumbling_kpi(enrich(read_orders(spark), restaurants)).orderBy("window_start", "city")
    rows = []
    for row in kpi.limit(limit).collect():
        item = row.asDict()
        item["window_start"] = str(item["window_start"])
        item["window_end"] = str(item["window_end"])
        item["revenue_mad"] = round(float(item["revenue_mad"]), 2)
        item["avg_basket_mad"] = round(float(item["avg_basket_mad"]), 2)
        rows.append(item)
    return rows


def run(spark, seconds: float = 30) -> dict:
    print(EXACTLY_ONCE)
    queries = start(spark)
    print("Requêtes :", [query.name for query in queries])
    print(
        "Les fichiers Delta en mode append n'apparaissent qu'une fois la fenêtre close "
        "(watermark 10 min + fenêtre 10 min). Le sink gold-live (update) montre les KPI tout de suite. "
        "Pour remplir l'or pendant la démo, rejouez l'historique :\n"
        "  python scripts/replay_kafka.py --file data/orders_history.jsonl "
        "--rate 400 --bootstrap localhost:9094"
    )
    time.sleep(seconds)
    for query in queries:
        query.stop()
    return {"queries": [query.name for query in queries]}


def main() -> dict:
    spark = build_spark("fastfood-partie6-production", kafka=True, delta=True)
    try:
        seconds = float(os.environ.get("FASTFOOD_STREAM_SECONDS", "30"))
        return run(spark, seconds)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()

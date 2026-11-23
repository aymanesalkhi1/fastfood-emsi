"""Partie 5 — Structured Streaming : Kafka, console, bronze, watermark."""

from __future__ import annotations

import json
import time

from pyspark.sql.functions import col, count, window

from .metrics import update_metrics
from .paths import lake, repo_root
from .session import build_spark, stop
from .streaming import bronze_frame, kafka_orders, parse_orders, start_console, start_sink
from .transforms import WATERMARK

LATE_NOTE = (
    "Un événement dont event_time a plus de 10 minutes de retard sur le max déjà vu "
    "est plus vieux que le watermark. Spark ne l'intègre pas dans les fenêtres déjà "
    "fermées : le recalcul infini n'est pas possible. Le producteur journalise un "
    "exemple à 15 minutes (outputs/late_example.json)."
)


def explain_late_event() -> dict | None:
    path = repo_root() / "outputs" / "late_example.json"
    if not path.exists():
        print("Pas encore d'exemple de retard. Lancez d'abord le mode fichier du producteur.")
        return None
    example = json.loads(path.read_text(encoding="utf-8"))
    print("Exemple tiré du générateur :")
    print(json.dumps(example, indent=2, ensure_ascii=False))
    print(LATE_NOTE)
    print(
        f"  event_time   = {example['event_time']}\n"
        f"  ingestion    = {example['ingest_time']}\n"
        f"  écart        = {example['delay_seconds']} s = 15 min\n"
        "  watermark    = max(event_time déjà vu) - 10 min\n"
        "  Les autres événements du micro-batch ont un event_time proche de l'ingestion,\n"
        "  donc le watermark dépasse cet event_time et la ligne est écartée."
    )
    return example


def start(spark) -> list:
    # Chaque sink a son propre readStream : Spark refuse deux requêtes sur la même source.
    counts = (
        parse_orders(kafka_orders(spark))
        .groupBy("city", "status")
        .agg(count("*").alias("orders"))
    )
    windowed = (
        parse_orders(kafka_orders(spark))
        .withWatermark("event_time", WATERMARK)
        .groupBy(window(col("event_time"), "10 minutes").alias("bucket"), col("city"))
        .agg(count("*").alias("orders"))
        .select(
            "city",
            col("bucket.start").alias("window_start"),
            col("bucket.end").alias("window_end"),
            "orders",
        )
    )
    return [
        start_console(parse_orders(kafka_orders(spark)), "part5-brut", mode="append", rows=8),
        start_sink(
            bronze_frame(kafka_orders(spark)),
            name="part5-bronze",
            fmt="json",
            path=lake("bronze", "orders_raw"),
            checkpoint=lake("checkpoints", "part5_bronze"),
            mode="append",
        ),
        start_console(counts, "part5-comptes", mode="complete", rows=30),
        start_console(windowed, "part5-fenetres", mode="update", rows=20),
    ]


def run(spark, seconds: float = 30) -> dict:
    example = explain_late_event()
    queries = start(spark)
    print(f"{len(queries)} requêtes démarrées pour {seconds:.0f} s. Regardez les micro-batches.")
    time.sleep(seconds)
    for query in queries:
        query.stop()
    payload = {
        "queries": [query.name for query in queries],
        "late_example": example,
        "late_note": LATE_NOTE,
        "bronze": lake("bronze", "orders_raw"),
    }
    update_metrics({"partie5": payload})
    return payload


def main() -> dict:
    spark = build_spark("fastfood-partie5-streaming", kafka=True, delta=True)
    try:
        seconds = float(__import__("os").environ.get("FASTFOOD_STREAM_SECONDS", "30"))
        return run(spark, seconds)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()

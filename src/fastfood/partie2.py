"""Partie 2 — SparkCore. map / filter / reduceByKey, puis le lineage."""

from __future__ import annotations

import time

from pyspark import RDD

from .io import read_orders_rdd
from .metrics import update_metrics
from .session import build_spark, stop

SHUFFLE_NOTE = (
    "filter et map sont étroites : pas de déplacement de données entre partitions. "
    "reduceByKey et aggregateByKey sont larges : Spark repartitionne par ville "
    "(HashPartitioner), écrit un shuffle, puis ouvre un nouveau stage pour la réduction. "
    "Le lineage (toDebugString) décrit ce plan. Si une partition est perdue, "
    "Spark rejoue uniquement les transformations mères, il ne reprend pas un fichier intermédiaire."
)


def delivered(raw: RDD) -> RDD:
    return raw.filter(lambda order: order["status"] == "DELIVERED")


def revenue_by_city(rows: RDD) -> list[tuple[str, float]]:
    totals = (
        rows.map(lambda order: (order["city"], float(order["amount_mad"])))
        .reduceByKey(lambda left, right: left + right)
    )
    return sorted(totals.collect(), key=lambda item: item[0])


def basket_by_city(rows: RDD) -> list[dict]:
    """Somme et nombre en un seul passage : un shuffle, pas deux."""
    zero = (0.0, 0)

    def seq(acc, amount):
        return (acc[0] + amount, acc[1] + 1)

    def comb(left, right):
        return (left[0] + right[0], right[1] + left[1])

    aggregated = (
        rows.map(lambda order: (order["city"], float(order["amount_mad"])))
        .aggregateByKey(zero, seq, comb)
    )
    result = []
    for city, (total, count) in aggregated.collect():
        result.append(
            {
                "city": city,
                "revenue_mad": round(total, 2),
                "orders": int(count),
                "avg_basket_mad": round(total / count, 2) if count else 0.0,
            }
        )
    return sorted(result, key=lambda item: item["city"])


def top_restaurants(rows: RDD, n: int = 5) -> list[dict]:
    counts = rows.map(lambda order: (order["restaurant_id"], 1)).reduceByKey(lambda a, b: a + b)
    top = counts.takeOrdered(n, key=lambda item: -item[1])
    return [{"restaurant_id": restaurant_id, "orders": int(count)} for restaurant_id, count in top]


def lineage_text(rdd: RDD) -> str:
    raw = rdd.toDebugString()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    return raw


def _timed(action):
    started = time.perf_counter()
    value = action()
    return value, round(time.perf_counter() - started, 3)


def run(spark) -> dict:
    raw = read_orders_rdd(spark).cache()
    raw.count()
    kept = delivered(raw).cache()
    revenue_rdd = kept.map(lambda order: (order["city"], float(order["amount_mad"]))).reduceByKey(
        lambda left, right: left + right
    )
    lineage = lineage_text(revenue_rdd)
    print("----- lineage (toDebugString) -----")
    print(lineage)
    print(SHUFFLE_NOTE)

    def _revenue():
        return revenue_by_city(kept)

    def _basket():
        return basket_by_city(kept)

    _, cold = _timed(_revenue)
    revenue, warm = _timed(_revenue)
    baskets, basket_seconds = _timed(_basket)
    ranking = top_restaurants(kept)

    print("\nChiffre d'affaires par ville (reduceByKey, sans dédoublonnage) :")
    for city, total in revenue:
        print(f"  {city:<12} {total:,.2f} MAD")
    print("\nPanier moyen (aggregateByKey, un seul passage) :")
    for row in baskets:
        print(
            f"  {row['city']:<12} CA={row['revenue_mad']:,.2f}  "
            f"commandes={row['orders']}  panier={row['avg_basket_mad']:,.2f}"
        )
    print("\nTop 5 restaurants (commandes livrées) :")
    for row in ranking:
        print(f"  {row['restaurant_id']}  {row['orders']}")

    raw.unpersist()
    kept.unpersist()
    by_city = {row["city"]: row["revenue_mad"] for row in baskets}
    for city, total in revenue:
        if abs(by_city[city] - round(total, 2)) > 0.05:
            raise RuntimeError(f"CA incohérent sur {city}")

    payload = {
        "revenue_by_city": [{"city": city, "revenue_mad": round(total, 2)} for city, total in revenue],
        "baskets": baskets,
        "top5": ranking,
        "seconds": {"revenue_cold": cold, "revenue_warm": warm, "basket": basket_seconds},
        "delivered_rows": kept.count(),
        "lineage_head": "\n".join(lineage.splitlines()[:12]),
        "shuffle": SHUFFLE_NOTE,
    }
    update_metrics({"partie2": payload})
    return payload


def main() -> dict:
    spark = build_spark("fastfood-partie2-rdd")
    try:
        return run(spark)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()

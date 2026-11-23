"""Partie 3 — DataFrames, SQL, jointure, UDF contre when/otherwise, Parquet."""

from __future__ import annotations

import time

from pyspark.sql.functions import broadcast, col, count, round as sqlround, sum as sqlsum

from .io import list_names, read_orders, read_restaurants
from .metrics import persist_enabled, update_metrics
from .paths import lake, repo_root
from .session import build_spark, stop
from .transforms import deduplicate, distance_band_column, distance_band_python_udf, enrich

UDF_NOTE = (
    "L'UDF Python sort du moteur : chaque ligne est sérialisée vers l'interpréteur, "
    "Catalyst ne peut ni réordonner ni éliminer le calcul, Tungsten ne le compile pas. "
    "when/otherwise est une expression native, visible dans le plan, et peut être "
    "combinée avec le reste de la requête."
)


def _delivered_total(frame) -> dict:
    row = (
        frame.filter(col("status") == "DELIVERED")
        .agg(
            sqlround(sqlsum("amount_mad"), 2).alias("revenue_mad"),
            count("*").alias("orders"),
        )
        .first()
    )
    revenue = row["revenue_mad"]
    return {
        "revenue_mad": float(revenue) if revenue is not None else 0.0,
        "orders": int(row["orders"] or 0),
    }


def _timed(action):
    started = time.perf_counter()
    value = action()
    return value, round(time.perf_counter() - started, 3)


def revenue_sql(spark, orders) -> list[dict]:
    orders.createOrReplaceTempView("orders_raw")
    frame = spark.sql(
        """
        SELECT city,
               ROUND(SUM(amount_mad), 2) AS revenue_mad,
               COUNT(*) AS orders
        FROM orders_raw
        WHERE status = 'DELIVERED'
        GROUP BY city
        ORDER BY city
        """
    )
    return [row.asDict() for row in frame.collect()]


def revenue_dataframe(orders) -> list[dict]:
    frame = (
        orders.filter(col("status") == "DELIVERED")
        .groupBy("city")
        .agg(
            sqlround(sqlsum("amount_mad"), 2).alias("revenue_mad"),
            count("*").alias("orders"),
        )
        .orderBy("city")
    )
    return [row.asDict() for row in frame.collect()]


def category_revenue(orders, restaurants) -> tuple[list[dict], str, int]:
    dimension = restaurants.select(
        "restaurant_id",
        col("name").alias("restaurant_name"),
        col("city").alias("restaurant_city"),
        "category",
    )
    # 200 restaurants : on force le BroadcastHashJoin, Catalyst le choisirait aussi.
    joined = orders.join(broadcast(dimension), "restaurant_id", "left")
    unmatched = joined.filter(col("category").isNull()).count()
    aggregated = (
        joined.filter(col("status") == "DELIVERED")
        .groupBy("city", "category")
        .agg(
            sqlround(sqlsum("amount_mad"), 2).alias("revenue_mad"),
            count("*").alias("orders"),
        )
        .orderBy(col("revenue_mad").desc())
    )
    plan = aggregated._jdf.queryExecution().toString()
    rows = [row.asDict() for row in aggregated.collect()]
    return rows, plan, int(unmatched)


def _band_seconds(orders) -> dict:
    cached = orders.select("distance_km").cache()
    cached.count()

    def run(frame):
        frame.groupBy("distance_band").count().collect()

    native = cached.withColumn("distance_band", distance_band_column())
    python = cached.withColumn("distance_band", distance_band_python_udf()(col("distance_km")))
    run(native)
    run(python)
    _, native_s = _timed(lambda: run(native))
    _, python_s = _timed(lambda: run(python))
    cached.unpersist()
    return {"when_otherwise_s": native_s, "python_udf_s": python_s}


def run(spark) -> dict:
    orders = read_orders(spark).cache()
    restaurants = read_restaurants(spark).cache()
    orders.count()
    restaurants.count()

    _, cold = _timed(lambda: revenue_dataframe(orders))
    by_frame, warm = _timed(lambda: revenue_dataframe(orders))
    by_sql = revenue_sql(spark, orders)
    by_category, plan, unmatched = category_revenue(orders, restaurants)
    bands = _band_seconds(orders)

    cleaned = deduplicate(enrich(orders, restaurants)).cache()
    gross = _delivered_total(orders)
    net = _delivered_total(cleaned)
    destination = lake("silver", "orders_history")
    (
        cleaned.write.mode("overwrite")
        .partitionBy("city")
        .parquet(destination)
    )
    written = spark.read.parquet(destination)
    partitions = [name for name in list_names(spark, destination) if name.startswith("city=")]

    orders.unpersist()
    restaurants.unpersist()
    cleaned.unpersist()
    explain_path = repo_root() / "outputs" / "partie3_explain.txt"
    if persist_enabled():
        explain_path.parent.mkdir(parents=True, exist_ok=True)
        explain_path.write_text(plan, encoding="utf-8")
    interesting = [
        line
        for line in plan.splitlines()
        if any(token in line for token in ("Join", "Broadcast", "Exchange", "Aggregate", "Adaptive"))
    ]

    print("CA DataFrame :", by_frame)
    print("CA SQL       :", by_sql)
    print("Restaurants sans correspondance :", unmatched)
    print("Partitions Parquet :", partitions)
    print("Temps UDF vs when :", bands)
    print(UDF_NOTE)
    print("----- extrait du plan -----")
    print("\n".join(interesting[:40]))

    payload = {
        "revenue_dataframe": by_frame,
        "revenue_sql": by_sql,
        "seconds": {"dataframe_cold": cold, "dataframe_warm": warm, **bands},
        "unmatched_restaurants": unmatched,
        "delivered_gross": gross,
        "delivered_deduped": net,
        "dedup_gap_mad": round(gross["revenue_mad"] - net["revenue_mad"], 2),
        "category_top": by_category,
        "parquet": destination,
        "parquet_rows": written.count(),
        "partitions": partitions,
        "plan_excerpt": interesting[:30],
        "udf_note": UDF_NOTE,
    }
    update_metrics({"partie3": payload})
    return payload


def main() -> dict:
    spark = build_spark("fastfood-partie3-sql", delta=True)
    try:
        return run(spark)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()

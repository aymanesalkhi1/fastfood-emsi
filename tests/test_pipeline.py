import os

from pyspark.sql.functions import col

from fastfood.contract import TIMESTAMP_FORMAT
from fastfood.generator import build_restaurants, generate_events, write_jsonl
from fastfood.io import read_orders
from fastfood.partie2 import run as run_rdd
from fastfood.partie3 import run as run_sql
from fastfood.partie4 import run as run_graph
from fastfood.schemas import ORDER_SCHEMA
from fastfood.streaming import start_sink
from fastfood.transforms import (
    courier_sessions,
    deduplicate,
    distance_band_column,
    distance_band_python_udf,
    sliding_cancel_rate,
    tumbling_kpi,
)


def test_batch_parts(spark):
    spark.catalog.clearCache()
    rdd = run_rdd(spark)
    assert {row["city"] for row in rdd["baskets"]} == {
        "Casablanca",
        "Rabat",
        "Marrakech",
        "Tanger",
    }
    assert rdd["top5"] == sorted(rdd["top5"], key=lambda row: -row["orders"])
    assert len(rdd["top5"]) == 5
    assert "reduceByKey" in rdd["lineage_head"] or "MapPartitionsRDD" in rdd["lineage_head"]

    sql = run_sql(spark)
    frame = {row["city"]: float(row["revenue_mad"]) for row in sql["revenue_dataframe"]}
    query = {row["city"]: float(row["revenue_mad"]) for row in sql["revenue_sql"]}
    assert frame == query
    assert sql["unmatched_restaurants"] == 0
    assert sql["parquet_rows"] > 0
    assert any(name.startswith("city=") for name in sql["partitions"])
    plan = "\n".join(sql["plan_excerpt"])
    assert "BroadcastHashJoin" in plan or "BroadcastExchange" in plan

    graph = run_graph(spark)
    assert graph["vertices"] > graph["edges"] > 0 or graph["edges"] > 0
    assert len(graph["top_in_degree"]) == 10
    assert len(graph["top_pagerank"]) == 10
    assert graph["components"] >= 4
    assert graph["aggregate_mismatch"] == 0
    assert graph["motif_sample"]
    assert graph["recommendations"]


def test_distance_band_udf_matches_native_expression(spark, history_file):
    orders = read_orders(spark, str(history_file))
    native = orders.groupBy(distance_band_column().alias("band")).count()
    python = orders.groupBy(distance_band_python_udf()(col("distance_km")).alias("band")).count()
    assert sorted(row.asDict()["band"] for row in native.collect()) == sorted(
        row.asDict()["band"] for row in python.collect()
    )


def test_windows_and_delta_restart(spark, tmp_path):
    restaurants = build_restaurants()
    events, _stats = generate_events(1600, restaurants, seed=7)
    source = tmp_path / "json"
    source.mkdir()
    write_jsonl(source / "part-a.json", events[:800])
    output = tmp_path / "parquet"
    checkpoint = tmp_path / "checkpoint"

    def start():
        frame = (
            spark.readStream.schema(ORDER_SCHEMA)
            .option("timestampFormat", TIMESTAMP_FORMAT)
            .json(str(source))
        )
        return (
            frame.writeStream.format("parquet")
            .option("path", str(output))
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )

    start().awaitTermination()
    assert spark.read.parquet(str(output)).count() == 800
    write_jsonl(source / "part-b.json", events[800:])
    start().awaitTermination()
    replayed = spark.read.parquet(str(output))
    assert replayed.count() == 1600

    batch = spark.read.schema(ORDER_SCHEMA).option("timestampFormat", TIMESTAMP_FORMAT).json(str(source))
    assert tumbling_kpi(batch).count() > 0
    assert sliding_cancel_rate(batch).count() > 0
    assert courier_sessions(batch).count() > 0

    gold = tmp_path / "gold"
    gold_checkpoint = tmp_path / "gold-checkpoint"
    stream = (
        spark.readStream.schema(ORDER_SCHEMA)
        .option("timestampFormat", TIMESTAMP_FORMAT)
        .json(str(source))
    )
    kpi = tumbling_kpi(deduplicate(stream))
    query = start_sink(
        kpi,
        name="test-gold",
        fmt="delta",
        path=str(gold),
        checkpoint=str(gold_checkpoint),
        available_now=True,
    )
    query.awaitTermination()
    stored = spark.read.format("delta").load(str(gold))
    assert stored.count() > 0
    distinct = stored.select("city", "window_start").distinct().count()
    assert stored.count() == distinct
    history = spark.sql(f"DESCRIBE HISTORY delta.`{gold}`")
    assert history.count() >= 1
    assert "operation" in history.columns

    from fastfood.partie6 import prove_restart

    proof = prove_restart(spark, root=str(tmp_path / "reprise"))
    assert proof["avant"] == proof["apres_redemarrage"]
    assert proof["apres_nouveaux_evenements"] > proof["avant"]
    assert proof["distinct_final"] == proof["apres_nouveaux_evenements"]

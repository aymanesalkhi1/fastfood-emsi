"""Sources Kafka et sinks. Une requête = un readStream = un checkpoint."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from .contract import TIMESTAMP_FORMAT, TOPIC_ORDERS
from .paths import lake
from .schemas import ORDER_SCHEMA
from .transforms import deduplicate, enrich


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def kafka_orders(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribe", TOPIC_ORDERS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("groupIdPrefix", "fastfood-ma")
        .option("maxOffsetsPerTrigger", os.getenv("FASTFOOD_MAX_OFFSETS", "200"))
        .load()
    )


def parse_orders(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.select(
        from_json(
            col("value").cast("string"),
            ORDER_SCHEMA,
            {"timestampFormat": TIMESTAMP_FORMAT, "mode": "PERMISSIVE"},
        ).alias("event")
    )
    return parsed.select("event.*").where(col("order_id").isNotNull())


def bronze_frame(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )


def orders_stream(spark: SparkSession, restaurants: DataFrame) -> DataFrame:
    """Nouveau consommateur à chaque appel. Le DataFrame de flux n'est pas réutilisable."""
    return enrich(deduplicate(parse_orders(kafka_orders(spark))), restaurants)


def start_console(
    df: DataFrame,
    name: str,
    mode: str = "append",
    trigger: str = "5 seconds",
    rows: int = 20,
) -> StreamingQuery:
    return (
        df.writeStream.queryName(name)
        .format("console")
        .outputMode(mode)
        .option("numRows", str(rows))
        .option("truncate", "false")
        .trigger(processingTime=trigger)
        .start()
    )


def start_sink(
    df: DataFrame,
    *,
    name: str,
    fmt: str,
    path: str,
    checkpoint: str,
    mode: str = "append",
    trigger: str = "10 seconds",
    partition_by: tuple[str, ...] = (),
    available_now: bool = False,
) -> StreamingQuery:
    writer = (
        df.writeStream.queryName(name)
        .format(fmt)
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .option("path", path)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger)
    return writer.start()


def checkpoint(name: str) -> str:
    return lake("checkpoints", name)

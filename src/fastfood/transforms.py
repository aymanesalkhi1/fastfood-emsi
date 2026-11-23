"""Transformations partagées entre le batch (parties 2-4) et le flux (parties 5-6)."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    session_window,
    sum as sqlsum,
    to_date,
    udf,
    unix_timestamp,
    when,
    window,
)
from pyspark.sql.types import StringType

from .bands import distance_band_value

WATERMARK = os.getenv("FASTFOOD_WATERMARK", "10 minutes")
TUMBLING = os.getenv("FASTFOOD_TUMBLING", "10 minutes")
SLIDE_WINDOW = os.getenv("FASTFOOD_SLIDE_WINDOW", "10 minutes")
SLIDE_EVERY = os.getenv("FASTFOOD_SLIDE_EVERY", "5 minutes")
SESSION_GAP = os.getenv("FASTFOOD_SESSION_GAP", "5 minutes")


def distance_band_column(column: str = "distance_km"):
    value = col(column)
    return (
        when(value.isNull(), lit(None).cast("string"))
        .when(value < 2, lit("court"))
        .when(value <= 5, lit("moyen"))
        .otherwise(lit("long"))
    )


def distance_band_python_udf():
    # UDF Python : une ligne = un appel interpréteur. when/otherwise reste dans Catalyst.
    return udf(distance_band_value, StringType())


def enrich(orders: DataFrame, restaurants: DataFrame) -> DataFrame:
    dimension = restaurants.select(
        "restaurant_id",
        col("name").alias("restaurant_name"),
        col("city").alias("restaurant_city"),
        "category",
        "latitude",
        "longitude",
    )
    joined = orders.join(dimension, "restaurant_id", "left")
    return joined.withColumn("distance_band", distance_band_column()).withColumn(
        "order_date", to_date(col("event_time"))
    )


def deduplicate(df: DataFrame, watermark: str | None = None) -> DataFrame:
    current = df
    if df.isStreaming:
        current = current.withWatermark("event_time", watermark or WATERMARK)
    return current.dropDuplicates(["order_id", "status"])


def tumbling_kpi(orders: DataFrame, duration: str | None = None) -> DataFrame:
    delivered = orders.filter(col("status") == "DELIVERED")
    bucket = window(col("event_time"), duration or TUMBLING).alias("bucket")
    return (
        delivered.groupBy(bucket, col("city"))
        .agg(
            count("*").alias("orders"),
            sqlsum("amount_mad").alias("revenue_mad"),
            avg("amount_mad").alias("avg_basket_mad"),
        )
        .select(
            "city",
            col("bucket.start").alias("window_start"),
            col("bucket.end").alias("window_end"),
            "orders",
            "revenue_mad",
            "avg_basket_mad",
        )
    )


def sliding_cancel_rate(
    orders: DataFrame,
    duration: str | None = None,
    slide: str | None = None,
) -> DataFrame:
    bucket = window(col("event_time"), duration or SLIDE_WINDOW, slide or SLIDE_EVERY).alias("bucket")
    return (
        orders.groupBy(bucket, col("city"))
        .agg(
            count("*").alias("events"),
            sqlsum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled"),
        )
        .select(
            "city",
            col("bucket.start").alias("window_start"),
            col("bucket.end").alias("window_end"),
            "events",
            "cancelled",
            (col("cancelled") / col("events")).alias("cancel_rate"),
        )
    )


def _seconds(duration: str) -> int:
    amount, unit = duration.split()
    factor = {
        "second": 1,
        "seconds": 1,
        "minute": 60,
        "minutes": 60,
        "hour": 3600,
        "hours": 3600,
    }[unit.lower()]
    return int(amount) * factor


def courier_sessions(orders: DataFrame, gap: str | None = None) -> DataFrame:
    """Session Spark : fin = dernier événement + inactivité.

    worked_seconds retire ce gap : c'est le temps entre le premier et le dernier événement.
    """
    gap_text = gap or SESSION_GAP
    session = session_window(col("event_time"), gap_text).alias("session")
    span = unix_timestamp(col("session.end")) - unix_timestamp(col("session.start"))
    return (
        orders.groupBy(col("courier_id"), session)
        .agg(count("*").alias("events"), sqlsum("amount_mad").alias("amount_mad"))
        .select(
            "courier_id",
            col("session.start").alias("window_start"),
            col("session.end").alias("window_end"),
            "events",
            "amount_mad",
            span.alias("session_span_seconds"),
            (span - lit(_seconds(gap_text))).alias("worked_seconds"),
        )
    )

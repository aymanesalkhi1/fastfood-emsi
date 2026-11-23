"""Schémas explicites. Pas d'inférence : le sujet l'interdit, et elle masque les erreurs."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .contract import TIMESTAMP_FORMAT

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("restaurant_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("city", StringType(), False),
        StructField("status", StringType(), False),
        StructField("amount_mad", DoubleType(), False),
        StructField("items_count", IntegerType(), False),
        StructField("distance_km", DoubleType(), False),
        StructField("courier_id", StringType(), True),
        StructField("event_time", TimestampType(), False),
    ]
)

RESTAURANT_SCHEMA = StructType(
    [
        StructField("restaurant_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("category", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
    ]
)

JSON_READ_OPTIONS = {
    "timestampFormat": TIMESTAMP_FORMAT,
    "mode": "FAILFAST",
}

"""Lecture des sources et petits accès au système de fichiers Hadoop."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .paths import orders_history_path, restaurants_path
from .schemas import JSON_READ_OPTIONS, ORDER_SCHEMA, RESTAURANT_SCHEMA


def _ensure(path: str) -> None:
    if path.startswith(("s3a://", "s3://", "file:")):
        return
    if not Path(path).exists():
        raise SystemExit(
            f"Fichier introuvable : {path}\n"
            "Générez-le avec : python producer/producer.py --mode file --n 50000 "
            "--restaurants data/restaurants.json --output data/orders_history.jsonl"
        )


def read_orders(spark: SparkSession, path: str | None = None) -> DataFrame:
    source = path or orders_history_path()
    _ensure(source)
    return spark.read.schema(ORDER_SCHEMA).options(**JSON_READ_OPTIONS).json(source)


def read_restaurants(spark: SparkSession, path: str | None = None) -> DataFrame:
    source = path or restaurants_path()
    _ensure(source)
    return (
        spark.read.schema(RESTAURANT_SCHEMA)
        .option("mode", "FAILFAST")
        .json(source)
    )


def read_orders_rdd(spark: SparkSession, path: str | None = None):
    source = path or orders_history_path()
    _ensure(source)
    return spark.sparkContext.textFile(source).map(json.loads)


def list_names(spark: SparkSession, uri: str) -> list[str]:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(path):
        return []
    return sorted(status.getPath().getName() for status in filesystem.listStatus(path))


def read_text(spark: SparkSession, uri: str) -> str:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = filesystem.open(path)
    return _read_all(jvm, stream)


def _read_all(jvm, stream) -> str:
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
    try:
        lines = []
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(line)
        return "\n".join(lines)
    finally:
        reader.close()

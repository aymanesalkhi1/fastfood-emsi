"""SparkSession commune : local pour les essais, standalone dans Docker."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from .paths import needs_s3, repo_root

SPARK_VERSION = "3.5.3"
JAVA_17 = Path("/usr/lib/jvm/java-17-openjdk-amd64")

PACKAGES = {
    "kafka": f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    "delta": "io.delta:delta-spark_2.12:3.2.0",
    "graph": "graphframes:graphframes:0.8.3-spark3.5-s_2.12",
    "s3": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
}


def _prefer_java_17() -> None:
    # Spark 3.5 est validé sur Java 17. Java 21 casse le démarrage du contexte.
    home = os.environ.get("FASTFOOD_JAVA_HOME")
    if not home and JAVA_17.exists():
        home = str(JAVA_17)
    if not home:
        return
    os.environ["JAVA_HOME"] = home
    os.environ["PATH"] = str(Path(home) / "bin") + os.pathsep + os.environ.get("PATH", "")


def _pythonpath() -> None:
    root = repo_root()
    for folder in (root / "src", root / "third_party"):
        entry = str(folder)
        if entry not in sys.path:
            sys.path.insert(0, entry)


def _driver_memory() -> None:
    # Le builder ignore souvent spark.driver.memory : la JVM est déjà dimensionnée
    # par spark-submit. Sans ça, le driver reste à 1 Go et GraphFrames sature.
    if "PYSPARK_SUBMIT_ARGS" not in os.environ:
        memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {memory} pyspark-shell"


def build_spark(
    app_name: str,
    *,
    graph: bool = False,
    delta: bool = False,
    kafka: bool = False,
) -> SparkSession:
    _prefer_java_17()
    _pythonpath()
    _driver_memory()
    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.warehouse.dir", "/tmp/fastfood-warehouse")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/fastfood-derby")
        .config("spark.ui.showConsoleProgress", "false")
    )
    ui = os.getenv("SPARK_UI_ENABLED", "false").lower() in {"1", "true", "yes"}
    builder = builder.config("spark.ui.enabled", "true" if ui else "false")
    if ui:
        builder = builder.config("spark.ui.port", os.getenv("SPARK_UI_PORT", "4040"))

    if master.startswith("spark://"):
        builder = (
            builder.config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "jupyter"))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", os.environ.get("SPARK_DRIVER_PORT", "7078"))
            .config("spark.blockManager.port", os.environ.get("SPARK_BLOCKMANAGER_PORT", "7079"))
        )

    if delta:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    if needs_s3():
        endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    if os.environ.get("SPARK_JARS_PRELOADED") != "1":
        selected = []
        if kafka:
            selected.append(PACKAGES["kafka"])
        if delta:
            selected.append(PACKAGES["delta"])
        if graph:
            selected.append(PACKAGES["graph"])
        if needs_s3():
            selected.append(PACKAGES["s3"])
        if selected:
            ivy = repo_root() / ".ivy2"
            ivy.mkdir(parents=True, exist_ok=True)
            builder = (
                builder.config("spark.jars.packages", ",".join(selected))
                .config("spark.jars.repositories", "https://repos.spark-packages.org")
                .config("spark.jars.ivy", str(ivy))
            )

    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def hold_ui(spark: SparkSession) -> None:
    seconds = int(os.getenv("SPARK_UI_HOLD_SECONDS", "0"))
    if seconds <= 0:
        return
    print(f"Spark UI : {spark.sparkContext.uiWebUrl} — maintenue {seconds} s pour la capture")
    time.sleep(seconds)


def stop(spark: SparkSession) -> None:
    hold_ui(spark)
    if os.getenv("SPARK_STOP", "1") != "0":
        spark.stop()

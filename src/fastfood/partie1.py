"""Partie 1 — la session est branchée sur le master standalone."""

from __future__ import annotations

from .metrics import update_metrics
from .session import build_spark, stop


def run(spark) -> dict:
    context = spark.sparkContext
    info = {
        "version": spark.version,
        "master": context.master,
        "app_name": context.appName,
        "default_parallelism": context.defaultParallelism,
    }
    print("Spark version          :", info["version"])
    print("Master                 :", info["master"])
    print("Application            :", info["app_name"])
    print("Cœurs (defaultParallelism) :", info["default_parallelism"])
    print("UI application         :", context.uiWebUrl)
    update_metrics({"partie1": info})
    return info


def main() -> dict:
    spark = build_spark("fastfood-partie1")
    try:
        return run(spark)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()

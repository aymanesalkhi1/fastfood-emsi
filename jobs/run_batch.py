#!/usr/bin/env python3
"""Enchaîne les parties 1 à 4 sur une seule SparkSession."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "third_party")]

from fastfood.partie1 import run as part1
from fastfood.partie2 import run as part2
from fastfood.partie3 import run as part3
from fastfood.partie4 import run as part4
from fastfood.session import build_spark, stop


def main() -> None:
    spark = build_spark("fastfood-batch", graph=True, delta=True)
    try:
        part1(spark)
        part2(spark)
        part3(spark)
        part4(spark)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()

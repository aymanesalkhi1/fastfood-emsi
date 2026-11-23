#!/usr/bin/env python3
"""Republie un JSONL déjà écrit dans Kafka.

producer.py est le fichier du sujet, sans mode de rejeu. Ce script envoie
les lignes telles quelles, avec le même client Kafka que le producteur.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def official_producer():
    path = ROOT / "producer" / "producer.py"
    spec = importlib.util.spec_from_file_location("official_producer", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser(description="Rejoue orders_history.jsonl dans Kafka")
    parser.add_argument("--file", default=str(ROOT / "data" / "orders_history.jsonl"))
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--rate", type=float, default=400, help="événements par seconde, 0 = sans pause")
    args = parser.parse_args()

    sink = official_producer().KafkaSink(args.bootstrap, args.topic)
    interval = 1.0 / args.rate if args.rate else 0
    sent = 0
    with open(args.file, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            sink.send(event["order_id"], line)
            sent += 1
            if interval:
                time.sleep(interval)
            if sent % 2000 == 0:
                print(f"rejoués={sent}", file=sys.stderr)
    sink.close()
    print(f"Terminé : {sent} événements rejoués vers {args.bootstrap} / {args.topic}", file=sys.stderr)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Repère l'événement le plus en retard du fichier officiel.

Le retard est l'écart entre event_time et le plus grand event_time déjà vu,
dans l'ordre d'arrivée. C'est ce que le watermark compare.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    source = Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "data" / "orders_history.jsonl")
    best = None
    seen = None
    late = over_10 = total = 0
    with source.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            total += 1
            event = json.loads(line)
            moment = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            if seen is not None and moment < seen:
                delay = int((seen - moment).total_seconds())
                late += 1
                if delay >= 600:
                    over_10 += 1
                if best is None or delay > best["delay_seconds"]:
                    best = {
                        "order_id": event["order_id"],
                        "status": event["status"],
                        "city": event["city"],
                        "event_time": event["event_time"],
                        "ingest_time": seen.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "delay_seconds": delay,
                        "watermark": "10 minutes",
                    }
            if seen is None or moment > seen:
                seen = moment
    if best is None:
        sys.exit("aucun événement en retard")
    destination = ROOT / "outputs" / "late_example.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(best, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(
        f"retards vus à l'arrivée : {late} ({late / total:.2%}), "
        f"au-delà de 10 min : {over_10}, exemple : {destination}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

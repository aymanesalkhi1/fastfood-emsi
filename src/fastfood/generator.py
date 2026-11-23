"""Générateur de commandes FastFood.ma.

Le sujet demande environ 5 % d'événements en retard (jusqu'à 15 minutes) et
environ 2 % de doublons (même order_id et même status). Le mode fichier est
déterministe (graine 42) pour que les chiffres du rapport soient reproductibles.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .contract import CITIES, EVENT_FIELDS, ISO_PATTERN, STATUSES, TOPIC_ORDERS
from .paths import repo_root

CITY_LAYOUT = (
    ("Casablanca", "CAS", 33.5731, -7.5898, 80),
    ("Rabat", "RAB", 34.0209, -6.8416, 50),
    ("Marrakech", "MAR", 31.6295, -7.9811, 40),
    ("Tanger", "TAN", 35.7595, -5.8340, 30),
)

QUARTIERS = {
    "Casablanca": [
        "Maarif", "Ain Diab", "Gauthier", "Bourgogne", "Anfa", "Oasis",
        "Californie", "Polo", "Belvédère", "Hermitage", "Racine", "CIL",
    ],
    "Rabat": [
        "Agdal", "Hassan", "Hay Riad", "Océan", "Souissi", "Aviation",
        "Orangers", "Akkari", "Yacoub El Mansour", "Diour Jamaa",
    ],
    "Marrakech": [
        "Guéliz", "Hivernage", "Médina", "Daoudiate", "Massira", "Semlalia",
        "Mhamid", "Sidi Youssef",
    ],
    "Tanger": [
        "Malabata", "Iberia", "Centre", "Boukhalef", "Marshan", "Mesnana",
        "California", "Bni Makada",
    ],
}

CATEGORIES = (
    "pizza", "tacos", "sushi", "marocain", "burger", "sandwich", "poisson", "poulet",
)

BRANDS = {
    "pizza": ["Pizza Roma", "Slice Club", "Four Atlas", "Napoli Express", "Casa Pizza"],
    "tacos": ["Tacos City", "Chhiwate Tacos", "Tacos Night", "Maxi Tacos", "Tacos Square"],
    "sushi": ["Sushi Wasabi", "Sakura Box", "Sushi House", "Maki Bar", "Tokyo Fresh"],
    "marocain": ["Dar Tajine", "Chez Mounir", "Snack Atlas", "Riad Express", "Kebda Time"],
    "burger": ["Burger Grill", "Smash House", "Burger & Co", "Grill 21", "Bun Theory"],
    "sandwich": ["Panini Club", "Sandwich Square", "Baguette Express", "Snack Minute"],
    "poisson": ["Mer & Filet", "Poisson Frais", "Casa Mer", "Filet Bleu"],
    "poulet": ["Poulet Doré", "Broasted House", "Chicken Grill", "Poulet Express"],
}

AMOUNT_RANGE = {
    "pizza": (55, 180),
    "tacos": (28, 90),
    "sushi": (85, 240),
    "marocain": (45, 160),
    "burger": (42, 145),
    "sandwich": (25, 75),
    "poisson": (60, 190),
    "poulet": (35, 120),
}

HISTORY_START = datetime(2026, 11, 8, 8, 0, tzinfo=timezone.utc)
HISTORY_END = datetime(2026, 11, 21, 22, 0, tzinfo=timezone.utc)
N_CUSTOMERS = 4000
N_COURIERS = 12


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime(ISO_PATTERN)


def build_restaurants(seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    seen_names: set[str] = set()
    serial = 1
    for city, code, lat, lon, count in CITY_LAYOUT:
        quartiers = QUARTIERS[city]
        for k in range(count):
            category = CATEGORIES[(serial + k) % len(CATEGORIES)]
            brand = BRANDS[category][(serial + k) % len(BRANDS[category])]
            quartier = quartiers[k % len(quartiers)]
            name = f"{brand} {quartier}"
            if name in seen_names:
                name = f"{brand} {quartier} {serial}"
            seen_names.add(name)
            rows.append(
                {
                    "restaurant_id": f"R-{serial:03d}",
                    "name": name,
                    "city": city,
                    "category": category,
                    "latitude": round(lat + rng.uniform(-0.05, 0.05), 6),
                    "longitude": round(lon + rng.uniform(-0.05, 0.05), 6),
                    "_code": code,
                }
            )
            serial += 1
    if len(rows) != 200:
        raise RuntimeError(f"référentiel invalide: {len(rows)} restaurants")
    return rows


def public_restaurant(row: dict) -> dict:
    return {key: value for key, value in row.items() if not key.startswith("_")}


def write_restaurants(path: Path, seed: int = 7) -> list[dict]:
    rows = build_restaurants(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(public_restaurant(row), ensure_ascii=False) + "\n")
    return rows


def load_restaurants(path: Path, seed: int = 7) -> list[dict]:
    if not path.exists():
        return write_restaurants(path, seed)
    rows = []
    codes = {city: code for city, code, *_rest in CITY_LAYOUT}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            row["_code"] = codes[row["city"]]
            rows.append(row)
    return rows


@dataclass
class Stamped:
    event: dict
    emit: datetime
    delay_s: int = 0


def _customers(rng: random.Random) -> dict[str, list[dict]]:
    weights = {city: count for city, _code, _lat, _lon, count in CITY_LAYOUT}
    total = sum(weights.values())
    by_city: dict[str, list[dict]] = defaultdict(list)
    serial = 1
    for city in CITIES:
        count = round(N_CUSTOMERS * weights[city] / total)
        group = [{"customer_id": f"C-{serial + i:05d}", "city": city} for i in range(count)]
        serial += count
        rng.shuffle(group)
        for rank, customer in enumerate(group):
            customer["weight"] = 1.0 / (rank + 1) ** 0.85
        by_city[city] = group
    return by_city


def _lifecycle(rng: random.Random) -> list[str]:
    if rng.random() < 0.08:
        if rng.random() < 0.6:
            return ["CREATED", "CANCELLED"]
        return ["CREATED", "PICKED_UP", "CANCELLED"]
    return ["CREATED", "PICKED_UP", "DELIVERED"]


def generate_events(
    n: int,
    restaurants: list[dict],
    seed: int = 42,
    start: datetime = HISTORY_START,
    end: datetime = HISTORY_END,
) -> tuple[list[dict], dict]:
    if n < 50:
        raise ValueError("n doit valoir au moins 50 pour illustrer retards et doublons")
    rng = random.Random(seed)
    customers = _customers(rng)
    ranked = restaurants[:]
    rng.shuffle(ranked)
    for rank, restaurant in enumerate(ranked):
        restaurant["_weight"] = 1.0 / (rank + 1) ** 0.65

    resto_weights = [restaurant["_weight"] for restaurant in restaurants]
    n_dup = round(0.02 * n)
    n_unique = n - n_dup
    horizon = end - timedelta(minutes=80)
    span = (horizon - start).total_seconds()

    stamped: list[Stamped] = []
    order_serial = 1
    while len(stamped) < n_unique:
        restaurant = rng.choices(restaurants, weights=resto_weights, k=1)[0]
        city = restaurant["city"]
        pool = customers[city]
        customer = rng.choices(pool, weights=[item["weight"] for item in pool], k=1)[0]
        low, high = AMOUNT_RANGE[restaurant["category"]]
        amount = round(rng.uniform(low, high), 2)
        items = rng.randint(1, 5)
        distance = round(rng.uniform(0.4, 11.0), 1)
        courier = f"L-{restaurant['_code']}-{rng.randint(1, N_COURIERS):02d}"
        base = start + timedelta(seconds=rng.random() * span)
        order_id = f"ORD-{order_serial:07d}"
        order_serial += 1
        cursor = base
        for status in _lifecycle(rng):
            if len(stamped) >= n_unique:
                break
            stamped.append(
                Stamped(
                    event={
                        "order_id": order_id,
                        "restaurant_id": restaurant["restaurant_id"],
                        "customer_id": customer["customer_id"],
                        "city": city,
                        "status": status,
                        "amount_mad": amount,
                        "items_count": items,
                        "distance_km": distance,
                        "courier_id": courier,
                    },
                    emit=cursor,
                )
            )
            cursor += timedelta(minutes=rng.randint(8, 25))

    stamped = stamped[:n_unique]
    n_late = max(2, round(0.05 * len(stamped)))
    late_indexes = rng.sample(range(len(stamped)), n_late)
    stamped[late_indexes[0]].delay_s = 900
    stamped[late_indexes[1]].delay_s = 720
    for index in late_indexes[2:]:
        stamped[index].delay_s = rng.randint(60, 900)

    for item in stamped:
        event_time = item.emit - timedelta(seconds=item.delay_s)
        item.event["event_time"] = iso(event_time)

    copies = []
    for _ in range(n_dup):
        source = stamped[rng.randrange(len(stamped))]
        copies.append(
            Stamped(
                event=dict(source.event),
                emit=source.emit + timedelta(seconds=rng.randint(1, 45)),
                delay_s=source.delay_s,
            )
        )
    final = sorted(stamped + copies, key=lambda item: item.emit)
    events = [item.event for item in final]

    late_15 = next(item for item in stamped if item.delay_s == 900)
    example = {
        "order_id": late_15.event["order_id"],
        "status": late_15.event["status"],
        "city": late_15.event["city"],
        "event_time": late_15.event["event_time"],
        "ingest_time": iso(late_15.emit),
        "delay_seconds": 900,
        "watermark": "10 minutes",
        "decision": (
            "event_time est 15 minutes derrière l'heure d'ingestion. "
            "Avec un watermark de 10 minutes, dès qu'un événement proche de "
            "l'heure d'ingestion a été vu, le watermark dépasse event_time "
            "et cet événement est écarté de l'agrégation fenêtrée."
        ),
    }
    late_lines = sum(1 for item in final if item.delay_s > 0)
    over_10 = sum(1 for item in final if item.delay_s >= 600)
    seen: set[tuple[str, str]] = set()
    duplicate_lines = 0
    for event in events:
        key = (event["order_id"], event["status"])
        if key in seen:
            duplicate_lines += 1
        else:
            seen.add(key)
    stats = {
        "lines": len(events),
        "duplicate_lines": duplicate_lines,
        "duplicate_rate": round(duplicate_lines / len(events), 4),
        "late_lines": late_lines,
        "late_rate": round(late_lines / len(events), 4),
        "late_over_10_min": over_10,
        "max_delay_seconds": max(item.delay_s for item in final),
        "late_example": example,
        "seed": seed,
    }
    return events, stats


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def _live_events(restaurants: list[dict], rng: random.Random):
    customers = _customers(rng)
    weights = []
    for rank, restaurant in enumerate(restaurants):
        restaurant["_weight"] = 1.0 / (rank + 1) ** 0.65
        weights.append(restaurant["_weight"])
    active: list[dict] = []
    serial = 1
    last: dict | None = None
    while True:
        if last is not None and rng.random() < 0.02:
            yield dict(last), True, 0
            continue
        if not active or rng.random() < 0.42:
            restaurant = rng.choices(restaurants, weights=weights, k=1)[0]
            pool = customers[restaurant["city"]]
            customer = rng.choices(pool, weights=[item["weight"] for item in pool], k=1)[0]
            low, high = AMOUNT_RANGE[restaurant["category"]]
            payload = {
                "order_id": f"ORD-{serial:07d}",
                "restaurant_id": restaurant["restaurant_id"],
                "customer_id": customer["customer_id"],
                "city": restaurant["city"],
                "amount_mad": round(rng.uniform(low, high), 2),
                "items_count": rng.randint(1, 5),
                "distance_km": round(rng.uniform(0.4, 11.0), 1),
                "courier_id": f"L-{restaurant['_code']}-{rng.randint(1, N_COURIERS):02d}",
            }
            serial += 1
            remaining = _lifecycle(rng)
            status = remaining.pop(0)
            if remaining:
                active.append({"payload": payload, "remaining": remaining})
        else:
            slot = active.pop(rng.randrange(len(active)))
            payload = slot["payload"]
            status = slot["remaining"].pop(0)
            if slot["remaining"]:
                active.append(slot)
        delay = rng.randint(60, 900) if rng.random() < 0.05 else 0
        now = datetime.now(timezone.utc)
        event = dict(payload)
        event["status"] = status
        event["event_time"] = iso(now - timedelta(seconds=delay))
        if status not in STATUSES:
            raise RuntimeError(status)
        last = event
        yield event, False, delay


def publish_kafka(
    restaurants: list[dict],
    bootstrap: str,
    topic: str,
    rate: float,
    duration: float,
    seed: int | None,
    from_file: str | None,
) -> dict:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit(
            "kafka-python n'est pas installé. Lancez: pip install -r requirements.txt"
        ) from exc

    producer = KafkaProducer(
        bootstrap_servers=[item.strip() for item in bootstrap.split(",") if item.strip()],
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
        retries=5,
    )
    sent = 0
    duplicates = 0
    late = 0
    printed_late = False
    deadline = time.monotonic() + duration if duration else None
    interval = 1.0 / rate if rate > 0 else 0
    next_tick = time.perf_counter()
    rng = random.Random(seed if seed is not None else time.time_ns())

    def emit(event: dict, is_dup: bool, delay: int) -> None:
        nonlocal sent, duplicates, late, printed_late, next_tick
        producer.send(topic, key=event["order_id"], value=event)
        sent += 1
        duplicates += int(is_dup)
        late += int(delay > 0)
        if delay >= 600 and not printed_late:
            printed_late = True
            print(
                "EXEMPLE RETARD "
                f"order_id={event['order_id']} status={event['status']} "
                f"delay_s={delay} event_time={event['event_time']} "
                f"wall={iso(datetime.now(timezone.utc))}"
            )
        if sent % 200 == 0:
            producer.flush()
            print(f"envoyés={sent} doublons={duplicates} retards={late}")
        if interval:
            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

    try:
        if from_file:
            with open(from_file, encoding="utf-8") as handle:
                for line in handle:
                    if deadline and time.monotonic() >= deadline:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    emit(json.loads(line), False, 0)
        else:
            for event, is_dup, delay in _live_events(restaurants, rng):
                if deadline and time.monotonic() >= deadline:
                    break
                emit(event, is_dup, delay)
    except KeyboardInterrupt:
        print("\narrêt demandé")
    finally:
        producer.flush()
        producer.close()
    summary = {"sent": sent, "duplicates": duplicates, "late": late, "topic": topic}
    print(f"terminé {summary}")
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Générateur de commandes FastFood.ma")
    parser.add_argument("--mode", choices=["file", "kafka"])
    parser.add_argument("--n", type=int, default=50000, help="Nombre de lignes en mode fichier")
    parser.add_argument("--rate", type=float, default=20, help="Événements par seconde en mode Kafka")
    parser.add_argument("--duration", type=float, default=0, help="Durée max du mode Kafka, 0 = infini")
    parser.add_argument("--out", default=str(repo_root() / "data" / "orders_history.jsonl"))
    parser.add_argument("--restaurants", default=str(repo_root() / "data" / "restaurants.json"))
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
    parser.add_argument("--topic", default=TOPIC_ORDERS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--from-file", default=None, help="Rejoue un JSONL dans Kafka")
    parser.add_argument("--init-restaurants", action="store_true")
    args = parser.parse_args(argv)

    restaurants_file = Path(args.restaurants)
    if args.init_restaurants:
        write_restaurants(restaurants_file)
        print(f"référentiel écrit: {restaurants_file} (200 lignes)")
        return

    restaurants = load_restaurants(restaurants_file)
    if args.mode is None:
        parser.error("--mode est requis, sauf avec --init-restaurants")

    if args.mode == "file":
        events, stats = generate_events(args.n, restaurants, seed=args.seed)
        destination = Path(args.out)
        write_jsonl(destination, events)
        example_path = repo_root() / "outputs" / "late_example.json"
        example_path.parent.mkdir(parents=True, exist_ok=True)
        example_path.write_text(
            json.dumps(stats["late_example"], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"écrit {destination} ({stats['lines']} lignes)")
        print(
            "doublons={duplicate_rate:.2%} retards={late_rate:.2%} "
            "retards>10min={late_over_10_min} exemple={example}".format(
                example=example_path,
                **stats,
            )
        )
        missing = [field for field in EVENT_FIELDS if field not in events[0]]
        if missing:
            raise SystemExit(f"champs manquants: {missing}")
        return

    publish_kafka(
        restaurants,
        bootstrap=args.bootstrap,
        topic=args.topic,
        rate=args.rate,
        duration=args.duration,
        seed=args.seed,
        from_file=args.from_file,
    )


if __name__ == "__main__":
    main()

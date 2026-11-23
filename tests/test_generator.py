import json
from collections import Counter
from pathlib import Path

from fastfood.bands import distance_band_value
from fastfood.contract import CITIES, EVENT_FIELDS, STATUSES
from fastfood.generator import build_restaurants, generate_events, load_restaurants
from fastfood.paths import repo_root


def test_distance_bands():
    assert distance_band_value(0.4) == "court"
    assert distance_band_value(1.99) == "court"
    assert distance_band_value(2) == "moyen"
    assert distance_band_value(5) == "moyen"
    assert distance_band_value(5.01) == "long"
    assert distance_band_value(None) is None


def test_restaurants_cover_the_four_cities():
    rows = build_restaurants()
    assert len(rows) == 200
    assert len({row["restaurant_id"] for row in rows}) == 200
    counts = Counter(row["city"] for row in rows)
    assert counts["Casablanca"] == 80
    assert counts["Rabat"] == 50
    assert counts["Marrakech"] == 40
    assert counts["Tanger"] == 30
    assert set(counts) == set(CITIES)


def test_committed_referential_is_the_official_file():
    disk = load_restaurants(repo_root() / "data" / "restaurants.json")
    assert len(disk) == 200
    assert len({row["restaurant_id"] for row in disk}) == 200
    counts = Counter(row["city"] for row in disk)
    assert counts == {"Casablanca": 85, "Rabat": 45, "Marrakech": 40, "Tanger": 30}
    assert set(Counter(row["category"] for row in disk)) == {
        "marocain", "pizza", "tacos", "burger", "poulet", "sandwich",
        "sushi", "asiatique", "healthy", "patisserie",
    }
    text = Path(repo_root() / "data" / "restaurants.json").read_text(encoding="utf-8").splitlines()
    assert len(text) == 200


def test_history_has_the_required_noise():
    restaurants = build_restaurants()
    events, stats = generate_events(4000, restaurants, seed=42)
    assert len(events) == 4000
    assert stats["duplicate_lines"] == round(0.02 * 4000)
    assert 0.035 <= stats["late_rate"] <= 0.08
    assert stats["max_delay_seconds"] == 900
    assert stats["late_over_10_min"] >= 2
    by_id = {row["restaurant_id"]: row for row in restaurants}
    seen = set()
    duplicates = 0
    for event in events:
        assert list(event) == list(EVENT_FIELDS)
        assert event["status"] in STATUSES
        assert event["city"] == by_id[event["restaurant_id"]]["city"]
        assert event["amount_mad"] > 0
        key = (event["order_id"], event["status"])
        duplicates += key in seen
        seen.add(key)
    assert duplicates == stats["duplicate_lines"]
    json.dumps(stats["late_example"])

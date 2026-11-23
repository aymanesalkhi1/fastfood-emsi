"""Chemins du dépôt et URI du Data Lake."""

from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def lake(*parts: str) -> str:
    base = os.environ.get("LAKE_URI", "s3a://lake").rstrip("/")
    tail = "/".join(part.strip("/") for part in parts if part)
    return f"{base}/{tail}" if tail else base


def orders_history_path() -> str:
    override = os.environ.get("ORDERS_HISTORY")
    if override:
        return override
    local = repo_root() / "data" / "orders_history.jsonl"
    if local.exists():
        return str(local)
    return lake("bronze", "history", "orders_history.jsonl")


def restaurants_path() -> str:
    override = os.environ.get("RESTAURANTS_PATH")
    if override:
        return override
    local = repo_root() / "data" / "restaurants.json"
    if local.exists():
        return str(local)
    return lake("bronze", "history", "restaurants.json")


def needs_s3(*extra: str) -> bool:
    blob = " ".join(
        [
            os.environ.get("LAKE_URI", ""),
            os.environ.get("ORDERS_HISTORY", ""),
            os.environ.get("RESTAURANTS_PATH", ""),
            *extra,
        ]
    )
    return "s3a://" in blob or "s3://" in blob

"""Petits résultats chiffrés réutilisés par le rapport et le tableau de bord."""

from __future__ import annotations

import json
import os
from typing import Any

from .paths import repo_root


def persist_enabled() -> bool:
    return os.environ.get("FASTFOOD_SKIP_METRICS") != "1"


def update_metrics(payload: dict[str, Any]) -> None:
    if not persist_enabled():
        return
    path = repo_root() / "outputs" / "metrics.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    current: dict[str, Any] = {}
    if path.exists():
        current = json.loads(path.read_text(encoding="utf-8"))
    current.update(payload)
    path.write_text(json.dumps(current, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def load_metrics() -> dict[str, Any]:
    path = repo_root() / "outputs" / "metrics.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

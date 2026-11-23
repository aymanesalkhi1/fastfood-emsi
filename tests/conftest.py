import os
from pathlib import Path

os.environ.setdefault("FASTFOOD_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
os.environ.setdefault("LAKE_URI", "file:///tmp/fastfood-lake-pytest")
os.environ.setdefault("SPARK_MASTER", "local[2]")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "4")
os.environ.setdefault("SPARK_UI_ENABLED", "false")
os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")
os.environ.setdefault("SPARK_STOP", "0")
os.environ.setdefault("FASTFOOD_SKIP_METRICS", "1")

import pytest

from fastfood.generator import build_restaurants, generate_events, write_jsonl


@pytest.fixture(scope="session")
def history_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    folder = tmp_path_factory.mktemp("history")
    destination = folder / "orders.jsonl"
    events, _stats = generate_events(6000, build_restaurants(), seed=42)
    write_jsonl(destination, events)
    os.environ["ORDERS_HISTORY"] = str(destination)
    return destination


@pytest.fixture(scope="session")
def spark(history_file):
    from fastfood.session import build_spark

    session = build_spark("fastfood-tests", graph=True, delta=True, kafka=True)
    yield session
    session.stop()

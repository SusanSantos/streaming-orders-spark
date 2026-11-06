"""Schema dos eventos e gerador sintético de pedidos (com atrasos e duplicatas)."""
from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("event_time", TimestampType(), False),
    ]
)

CATEGORIES = ["eletronicos", "moda", "casa", "livros", "esportes"]


def generate_events(
    n: int,
    start: datetime | None = None,
    late_ratio: float = 0.05,
    dup_ratio: float = 0.03,
    bad_ratio: float = 0.02,
    seed: int = 42,
) -> list[dict]:
    """Gera n eventos. Parte chega atrasada, duplicada ou inválida (amount < 0)."""
    rnd = random.Random(seed)
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    events: list[dict] = []
    for i in range(n):
        ts = start + timedelta(seconds=i * 2)
        if rnd.random() < late_ratio:
            ts -= timedelta(minutes=rnd.randint(30, 90))  # evento muito atrasado
        amount = round(rnd.uniform(10, 900), 2)
        if rnd.random() < bad_ratio:
            amount = -abs(amount)
        events.append(
            {
                "event_id": str(uuid.UUID(int=rnd.getrandbits(128))),
                "order_id": f"O{i:07d}",
                "customer_id": f"C{rnd.randint(1, 500):04d}",
                "category": rnd.choice(CATEGORIES),
                "amount": amount,
                "quantity": rnd.randint(1, 5),
                "event_time": ts.isoformat(),
            }
        )
    dups = [dict(e) for e in events if rnd.random() < dup_ratio]
    events.extend(dups)  # mesmo event_id reenviado (at-least-once)
    return events


def write_json_batches(events: list[dict], out_dir: str, batch_size: int = 500) -> int:
    """Simula o tópico Kafka gravando micro-lotes JSON-lines (fonte local do Spark)."""
    import os

    os.makedirs(out_dir, exist_ok=True)
    n_files = 0
    for i in range(0, len(events), batch_size):
        path = os.path.join(out_dir, f"batch_{i // batch_size:05d}.json")
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(e) + "\n" for e in events[i : i + batch_size])
        n_files += 1
    return n_files

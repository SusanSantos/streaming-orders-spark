"""Produtor: publica no Kafka (se KAFKA_BOOTSTRAP) ou grava lotes JSON locais."""
from __future__ import annotations

import argparse
import json
import os

from src.events import generate_events, write_json_batches


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=5000)
    ap.add_argument("--out", default="data/landing")
    a = ap.parse_args()
    events = generate_events(a.n)
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    if bootstrap:
        from kafka import KafkaProducer  # pip install kafka-python

        p = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode())
        for e in events:
            p.send(os.getenv("KAFKA_TOPIC", "orders"), e)
        p.flush()
        print(f"{len(events)} eventos publicados no Kafka")
    else:
        files = write_json_batches(events, a.out)
        print(f"{len(events)} eventos em {files} arquivos ({a.out})")


if __name__ == "__main__":
    main()

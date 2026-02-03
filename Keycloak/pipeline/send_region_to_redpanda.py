#!/usr/bin/env python3
"""
send_region_to_redpanda.py

Selects downloaded data by region (from manifest.jsonl) and sends messages to Redpanda.
Prints success for each produced message.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from kafka import KafkaProducer


def load_manifest(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Manifest not found: {path}")
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default="./data/manifest.jsonl")
    ap.add_argument("--region", required=True)
    ap.add_argument("--topic", default="solar_kataster_hessen")
    ap.add_argument("--brokers", default="localhost:1111")
    ap.add_argument("--client-id", default="solar-producer")
    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    rows = load_manifest(manifest_path)

    selected = [r for r in rows if r.get("region") == args.region]
    if not selected:
        print(f"[info] No records for region={args.region} in {manifest_path}")
        sys.exit(0)

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        client_id=args.client_id,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=10,
        retries=5,
    )

    ok = 0
    for i, rec in enumerate(selected, start=1):
        key = rec.get("region", "region")
        future = producer.send(args.topic, key=key, value=rec)
        try:
            md = future.get(timeout=30)
            ok += 1
            print(
                f"[sent] {i}/{len(selected)} topic={md.topic} partition={md.partition} offset={md.offset} region={key}"
            )
        except Exception as e:
            print(f"[error] failed to send record {i}/{len(selected)}: {e}", file=sys.stderr)

    producer.flush(timeout=30)
    producer.close()

    print(f"[done] sent_ok={ok} total_selected={len(selected)} topic={args.topic}")


if __name__ == "__main__":
    main()


from __future__ import annotations

import argparse
import json
from pathlib import Path

from ml.scoring import score_entities


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo real-time scoring with shared model artifacts and Redis-served features.")
    parser.add_argument("--customer-id", type=int, required=True)
    parser.add_argument("--campaign-id", type=int, required=True)
    parser.add_argument("--advertiser-id", type=int)
    parser.add_argument("--customer-manifest")
    parser.add_argument("--campaign-manifest")
    parser.add_argument("--advertiser-manifest")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--write-redis", action="store_true")
    parser.add_argument("--output-path")
    args = parser.parse_args()

    result = score_entities(
        customer_id=args.customer_id,
        campaign_id=args.campaign_id,
        advertiser_id=args.advertiser_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        customer_manifest=str(Path(args.customer_manifest)) if args.customer_manifest else None,
        campaign_manifest=str(Path(args.campaign_manifest)) if args.campaign_manifest else None,
        advertiser_manifest=str(Path(args.advertiser_manifest)) if args.advertiser_manifest else None,
        write_redis=args.write_redis,
    )

    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output_path:
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    print("Deprecated: use python3 scripts/demo_realtime_scoring.py")
    main()

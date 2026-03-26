from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ml.inference import latest_manifest
from ml.scoring import score_advertiser, score_campaign, score_customer


app = FastAPI(title="ML Inference Service", version="1.0.0")


class CustomerScoreRequest(BaseModel):
    customer_id: int
    write_redis: bool = False


class CampaignScoreRequest(BaseModel):
    campaign_id: int
    write_redis: bool = False


class AdvertiserScoreRequest(BaseModel):
    advertiser_id: int
    write_redis: bool = False


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/models/latest")
def latest_models() -> dict[str, str]:
    try:
        return {
            "customer_realtime": str(latest_manifest("customer_realtime")),
            "campaign": str(latest_manifest("campaign")),
            "advertiser": str(latest_manifest("advertiser")),
        }
    except Exception as exc:  # pragma: no cover - operational path
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/score/customer_purchase")
def score_customer_purchase(request: CustomerScoreRequest) -> dict:
    try:
        return score_customer(
            customer_id=request.customer_id,
            redis_host="redis.data-platform-serve",
            redis_port=6379,
            write_redis=request.write_redis,
        )
    except Exception as exc:  # pragma: no cover - operational path
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/score/campaign_success")
def score_campaign_success(request: CampaignScoreRequest) -> dict:
    try:
        return score_campaign(
            campaign_id=request.campaign_id,
            redis_host="redis.data-platform-serve",
            redis_port=6379,
            write_redis=request.write_redis,
        )
    except Exception as exc:  # pragma: no cover - operational path
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/score/advertiser_budget_expansion")
def score_advertiser_budget_expansion(request: AdvertiserScoreRequest) -> dict:
    try:
        return score_advertiser(
            advertiser_id=request.advertiser_id,
            redis_host="redis.data-platform-serve",
            redis_port=6379,
            write_redis=request.write_redis,
        )
    except Exception as exc:  # pragma: no cover - operational path
        raise HTTPException(status_code=500, detail=str(exc)) from exc

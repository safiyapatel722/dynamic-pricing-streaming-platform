"""
serving/api.py

FastAPI serving layer — exposes real-time surge pricing via REST API.
Reads surge data from Redis — shared with consumer, always current.
"""

from fastapi import FastAPI, HTTPException
from config.settings import settings
from utils.redis_client import get_surge
from utils.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="Dynamic Pricing API")


@app.get("/surge")
def get_surge_endpoint(location: str) -> dict:
    """
    Returns current surge multiplier for a given location.
    Reads from Redis — written by consumer after every event processed.

    Example: GET /surge?location=Baner
    """
    if location not in settings.locations:
        raise HTTPException(
            status_code=404,
            detail=f"Location '{location}' not found. "
                   f"Valid locations: {settings.locations}"
        )

    data = get_surge(location)

    # no data yet — consumer hasn't processed any events for this location
    if data is None:
        return {
            "location": location,
            "surge_multiplier": 1.0,
            "riders": 0,
            "drivers": 0,
            "note": "no events received yet"
        }

    return {
        "location": location,
        **data
    }


@app.get("/health")
def health_check() -> dict:
    """Health check — used by GCP Cloud Run to verify service is alive."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
"""
serving/api.py

FastAPI serving layer — exposes real-time surge pricing via REST API.
Reads current windowed state from StateManager and returns surge multiplier.
"""

from fastapi import FastAPI, HTTPException
from processing.state_manager import StateManager
from processing.surge_calculator import SurgeCalculator
from config.settings import settings


app = FastAPI(title="Dynamic Pricing API")

# shared instances — same state_manager used by consumer and API
state_manager = StateManager()
surge_calculator = SurgeCalculator()


@app.get("/surge")
def get_surge(location: str) -> dict:
    """
    Returns current surge multiplier for a given location.

    Example: GET /surge?location=Baner
    Response: {
        "location": "Baner",
        "surge_multiplier": 2.3,
        "riders": 45,
        "drivers": 18
    }
    """
    if location not in settings.locations:
        raise HTTPException(
            status_code=404,
            detail=f"Location '{location}' not found. "
                   f"Valid locations: {settings.locations}"
        )

    counts = state_manager.get_counts(location)
    surge = surge_calculator.calculate(counts["riders"], counts["drivers"])

    return {
        "location": location,
        "surge_multiplier": surge,
        "riders": counts["riders"],
        "drivers": counts["drivers"]
    }


@app.get("/health")
def health_check() -> dict:
    """Health check endpoint — used by GCP Cloud Run to verify service is alive."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
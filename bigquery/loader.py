"""
bigquery/loader.py

Loads surge analytics data from Redis into BigQuery surge_analytics table.
Runs as a scheduled job every 5 minutes — micro-batch pattern.

Why micro-batch over streaming insert:
- Dashboards don't need millisecond freshness
- Streaming inserts cost 10x more than batch loads
- Batch loads are more reliable and easier to monitor
"""

import schedule
import time
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from utils.redis_client import get_surge
from utils.logger import get_logger
from config.settings import settings
from bigquery.schema import get_client, DATASET_ID, SURGE_TABLE_ID

logger = get_logger(__name__)


def is_peak_hour(hour: int) -> bool:
    """Check if given hour falls within peak hours."""
    for start, end in settings.peak_hours:
        if start.hour <= hour < end.hour:
            return True
    return False


def load_surge_snapshot() -> None:
    """
    Reads current surge for all locations from Redis
    and inserts a snapshot row into BigQuery.
    Runs every 5 minutes via scheduler.
    """
    client = get_client()
    table_ref = f"{settings.gcp_project_id}.{DATASET_ID}.{SURGE_TABLE_ID}"

    rows = []
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=settings.window_size_sec)

    for location in settings.locations:
        data = get_surge(location)

        if data is None:
            continue

        rows.append({
            "location": location,
            "surge_multiplier": data["surge_multiplier"],
            "rider_count": data["riders"],
            "driver_count": data["drivers"],
            "hour_of_day": now.hour,
            "is_peak_hour": is_peak_hour(now.hour),
            "recorded_at": now.isoformat(),
            "window_start": window_start.isoformat(),
        })

    if not rows:
        logger.info("No surge data in Redis yet — skipping load")
        return

    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
    else:
        logger.info(f"Loaded {len(rows)} surge snapshots to BigQuery")


def run_loader() -> None:
    """
    Runs surge snapshot loader every 5 minutes.
    schedule library keeps it simple — no Airflow needed
    for a single recurring job.
    """
    logger.info("Starting BigQuery loader — runs every 5 minutes")

    # run immediately on startup
    load_surge_snapshot()

    # then every 5 minutes
    schedule.every(5).minutes.do(load_surge_snapshot)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    run_loader()
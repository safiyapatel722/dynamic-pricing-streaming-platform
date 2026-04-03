"""
storage/archiver.py

Subscribes to GCP Pub/Sub and archives raw events to GCS as Parquet.
Includes:
- batching
- time-based flushing
- retry with exponential backoff
- rate-limit handling (429 safe)
"""

import json
import io
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1, storage
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import settings


# -----------------------------
# CLIENTS (created once)
# -----------------------------
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()

subscription_path = subscriber.subscription_path(
    settings.gcp_project_id,
    settings.pubsub_archiver_subscription
)


# -----------------------------
# BUFFER CONFIG
# -----------------------------
BATCH_SIZE = 300           # increased to reduce API calls
FLUSH_INTERVAL = 10        # seconds

event_buffer = []
LAST_FLUSH_TIME = time.time()


# -----------------------------
# CORE LOGIC
# -----------------------------
def _flush_to_gcs(events: list) -> None:
    """
    Writes events to GCS as Parquet with retry + exponential backoff.
    """
    retries = 3

    for attempt in range(retries):
        try:
            table = pa.table({
                "event_id":   [e["event_id"] for e in events],
                "event_type": [e["event_type"] for e in events],
                "location":   [e["location"] for e in events],
                "event_time": [e["event_time"] for e in events],
            })

            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            bucket = storage_client.bucket(settings.gcs_bucket)

            # unique file name to avoid overwrite
            now = datetime.now(timezone.utc)
            blob_path = (
                f"events/{now.strftime('%Y-%m-%d')}/"
                f"{now.strftime('%H')}/"
                f"batch_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
            )

            blob = bucket.blob(blob_path)
            blob.upload_from_file(buffer, content_type="application/octet-stream")

            print(f"Flushed {len(events)} events → gs://{settings.gcs_bucket}/{blob_path}")
            return

        except Exception as e:
            print(f"[Retry {attempt+1}] Failed to upload: {e}")

            # exponential backoff
            time.sleep(2 ** attempt)

    print("❌ Final failure after retries — events dropped")


def archive_message(message) -> None:
    """
    Pub/Sub callback — buffers events and flushes to GCS.
    Handles both batch-size and time-based flushing.
    """
    global LAST_FLUSH_TIME

    try:
        data = json.loads(message.data.decode("utf-8"))
        event_buffer.append(data)

        current_time = time.time()

        # flush conditions:
        if (
            len(event_buffer) >= BATCH_SIZE
            or (current_time - LAST_FLUSH_TIME) >= FLUSH_INTERVAL
        ):
            _flush_to_gcs(event_buffer.copy())
            event_buffer.clear()
            LAST_FLUSH_TIME = current_time

        message.ack()

    except Exception as e:
        print(f"Failed to archive message: {e}")
        # do not ack → Pub/Sub will retry


def run_archiver() -> None:
    """
    Start streaming pull for archiving.
    Ensures buffer flush on shutdown.
    """
    print(f"Archiver listening on: {subscription_path}")

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=archive_message
    )

    try:
        streaming_pull_future.result()

    except KeyboardInterrupt:
        print("Stopping archiver... flushing remaining events")

        if event_buffer:
            _flush_to_gcs(event_buffer)

        streaming_pull_future.cancel()
        print("Archiver stopped.")


if __name__ == "__main__":
    run_archiver()
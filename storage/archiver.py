"""
storage/archiver.py

Subscribes to GCP Pub/Sub and archives raw events to GCS as Parquet.
Batches events and flushes on size or time threshold.
"""

import io
import time
from datetime import datetime, timezone
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import settings
from utils.serializer import deserialize
from utils.pubsub_helper import get_subscriber
from utils.logger import get_logger

logger = get_logger(__name__)

# clients — created once
storage_client = storage.Client()
subscriber, subscription_path = get_subscriber(
    settings.pubsub_subscription_archiver
)

settings.archiver_batch_size
settings.archiver_flush_interval


event_buffer = []
LAST_FLUSH_TIME = time.time()


def _flush_to_gcs(events: list) -> None:
    """Write buffered events to GCS as Parquet with retry + backoff."""
    for attempt in range(settings.archiver_max_retries):
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

            now = datetime.now(timezone.utc)
            blob_path = (
                f"events/{now.strftime('%Y-%m-%d')}/"
                f"{now.strftime('%H')}/"
                f"batch_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
            )

            bucket = storage_client.bucket(settings.gcs_bucket)
            blob = bucket.blob(blob_path)
            blob.upload_from_file(buffer, content_type="application/octet-stream")

            logger.info(f"Flushed {len(events)} events → gs://{settings.gcs_bucket}/{blob_path}")
            return

        except Exception as e:
            logger.warning(f"Retry {attempt + 1} — upload failed: {e}")
            time.sleep(2 ** attempt)

    logger.error("Final failure after retries — events dropped")


def archive_message(message) -> None:
    """Pub/Sub callback — buffers events and flushes to GCS."""
    global LAST_FLUSH_TIME

    try:
        data = deserialize(message.data)
        event_buffer.append(data)

        current_time = time.time()

        if (
            len(event_buffer) >= settings.archiver_batch_size
            or (current_time - LAST_FLUSH_TIME) >= settings.archiver_flush_interval
        ):
            _flush_to_gcs(event_buffer.copy())
            event_buffer.clear()
            LAST_FLUSH_TIME = current_time

        message.ack()

    except Exception as e:
        logger.error(f"Failed to archive message: {e}")


def run_archiver() -> None:
    """Start streaming pull for archiving."""
    logger.info(f"Archiver listening on: {subscription_path}")

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=archive_message
    )

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        if event_buffer:
            _flush_to_gcs(event_buffer)
        streaming_pull_future.cancel()
        logger.info("Archiver stopped.")


if __name__ == "__main__":
    run_archiver()
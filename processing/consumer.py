"""
processing/consumer.py

Pulls messages from GCP Pub/Sub, reconstructs Event objects,
updates windowed state, calculates surge and writes to Redis.
"""

from datetime import datetime
from config.settings import settings
from models.event import Event
from processing.state_manager import StateManager
from processing.surge_calculator import SurgeCalculator
from utils.serializer import deserialize
from utils.pubsub_helper import get_subscriber
from utils.redis_client import set_surge
from utils.logger import get_logger

logger = get_logger(__name__)

# module level — created once, reused for every message
subscriber, subscription_path = get_subscriber(
    settings.pubsub_subscription_consumer
)
state_manager = StateManager()
surge_calculator = SurgeCalculator()


def process_message(message) -> None:
    """
    Callback triggered by Pub/Sub for every incoming message.
    Decodes bytes → Event → state update → surge → Redis write.
    """
    try:
        # bytes → dict
        data = deserialize(message.data)

        # reconstruct Event with original event_id — critical for idempotency
        event = Event(
            event_type=data["event_type"],
            location=data["location"],
            event_time=datetime.fromisoformat(data["event_time"]),
            event_id=data["event_id"]
        )

        # update windowed state — duplicates safely ignored inside
        state_manager.process_event(event)

        # calculate surge and write to Redis
        counts = state_manager.get_counts(event.location)
        surge = surge_calculator.calculate(counts["riders"], counts["drivers"])

        set_surge(event.location, surge, counts["riders"], counts["drivers"])

        logger.info(
            f"{event.location} → "
            f"riders: {counts['riders']} "
            f"drivers: {counts['drivers']} → "
            f"surge: {surge}x"
        )

        # acknowledge — without this Pub/Sub redelivers indefinitely
        message.ack()

    except Exception as e:
        # don't ack on failure — Pub/Sub will redeliver
        logger.error(f"Failed to process message: {e}")


def run_consumer() -> None:
    """
    Start streaming pull from Pub/Sub.
    subscriber.subscribe() is non-blocking — result() keeps main thread alive.
    """
    logger.info(f"Consumer listening on: {subscription_path}")

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=process_message
    )

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    run_consumer()
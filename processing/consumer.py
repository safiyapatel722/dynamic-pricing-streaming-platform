"""
processing/consumer.py

Pulls messages from GCP Pub/Sub, reconstructs Event objects,
updates windowed state, and calculates real-time surge pricing.
"""

from google.cloud import pubsub_v1
import json
from datetime import datetime, timezone
from config.settings import settings
from models.event import Event
from processing.state_manager import StateManager
from processing.surge_calculator import SurgeCalculator


# module level — created once, reused for every message
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    settings.gcp_project_id,
    settings.pubsub_subscription_consumer
)
state_manager = StateManager()
surge_calculator = SurgeCalculator()


def process_message(message) -> None:
    """
    Callback triggered by Pub/Sub for every incoming message.
    Decodes bytes → Event → state update → surge calculation.
    """
    try:
        # step 1: bytes → dict
        data = json.loads(message.data.decode("utf-8"))

        # step 2: reconstruct Event with original event_id and event_time
        # fromisoformat() reverses .isoformat() from Event.to_dict()
        # event_id must match original — critical for idempotency checks
        event = Event(
            event_type=data["event_type"],
            location=data["location"],
            event_time=datetime.fromisoformat(data["event_time"]),
            event_id=data["event_id"]
        )

        # step 3: update windowed state
        # idempotency check lives inside state_manager — duplicates safely ignored
        state_manager.process_event(event)

        # step 4: calculate surge and log result
        counts = state_manager.get_counts(event.location)
        surge = surge_calculator.calculate(counts["riders"], counts["drivers"])

        print(
            f"{event.location} → "
            f"riders: {counts['riders']} "
            f"drivers: {counts['drivers']} → "
            f"surge: {surge}x"
        )

        # step 5: acknowledge — without this Pub/Sub redelivers indefinitely
        message.ack()

    except Exception as e:
        # don't ack on failure — Pub/Sub will redeliver
        # idempotency in StateManager handles the duplicate safely
        print(f"Failed to process message: {e}")


def run_consumer() -> None:
    """
    Start streaming pull from Pub/Sub.
    subscriber.subscribe() is non-blocking — result() keeps main thread alive.
    """
    print(f"Consumer listening on: {subscription_path}")

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=process_message
    )

    try:
        # blocks main thread — consumer runs until interrupted
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Consumer stopped.")


if __name__ == "__main__":
    run_consumer()
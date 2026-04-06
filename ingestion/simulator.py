"""
ingestion/simulator.py

Generates synthetic ride events with peak-hour weighting
and publishes them to GCP Pub/Sub.
"""

import random
import time
from datetime import datetime, timezone
from models.event import Event
from config.settings import settings
from utils.serializer import serialize
from utils.pubsub_helper import get_publisher
from utils.logger import get_logger

logger = get_logger(__name__)

# created once at module level — expensive to recreate per event
publisher, topic_path = get_publisher()


def is_peak_hour(current_time) -> bool:
    """Check if current time falls within any configured peak hour range."""
    for start, end in settings.peak_hours:
        if start <= current_time <= end:
            return True
    return False


def generate_event() -> Event:
    """
    Generate a single ride event with peak-hour weighted probabilities.
    During peak hours riders are favored — simulates real demand spikes.
    """
    current_time = datetime.now(timezone.utc).time()
    is_pk = is_peak_hour(current_time)

    # weight pool driven by config — no magic numbers in code
    if is_pk:
        pool = ["rider"] * settings.rider_peak_weight + ["driver"] * settings.driver_peak_weight
    else:
        pool = ["rider"] * settings.rider_offpeak_weight + ["driver"] * settings.driver_offpeak_weight

    event_type = random.choice(pool)
    location = random.choice(settings.locations)

    return Event(
        event_type=event_type,
        location=location,
        event_time=datetime.now(timezone.utc)
    )


def publish_event(event: Event) -> None:
    """Serialize event to bytes and publish to GCP Pub/Sub."""
    publisher.publish(topic_path, data=serialize(event))
    logger.info(f"Published: {event.event_type} @ {event.location} [{event.event_id}]")


def run_simulation() -> None:
    """Continuously generate and publish events."""
    logger.info(f"Starting simulation → topic: {topic_path}")

    while True:
        event = generate_event()
        publish_event(event)
        time.sleep(settings.event_interval_sec)


if __name__ == "__main__":
    run_simulation()
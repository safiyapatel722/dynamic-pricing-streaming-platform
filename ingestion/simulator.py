"""
ingestion/simulator.py

Generates synthetic ride events (rider requests + driver availability)
with peak-hour weighting and publishes them to GCP Pub/Sub.
"""

import random
import json
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from models.event import Event
from config.settings import settings


# created once at module level — expensive to recreate per event
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(settings.gcp_project_id, settings.pubsub_topic)


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
    """
    Serialize event to JSON bytes and publish to GCP Pub/Sub topic.
    Fire and forget — Pub/Sub handles delivery guarantees.
    """
    message_bytes = json.dumps(event.to_dict()).encode("utf-8")
    publisher.publish(topic_path, data=message_bytes)


def run_simulation() -> None:
    """
    Continuously generate and publish events.
    Sleep interval controls throughput — lower = more events per second.
    """
    print(f"Starting simulation → topic: {topic_path}")

    while True:
        event = generate_event()
        publish_event(event)
        print(f"Published: {event.event_type} @ {event.location} [{event.event_id}]")
        time.sleep(0.5)  # 2 events/sec — enough to fill 60s window meaningfully


if __name__ == "__main__":
    run_simulation()
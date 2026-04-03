"""
processing/state_manager.py

Maintains windowed state per location — counts riders and drivers
within the last window_size_sec seconds for surge calculation.
Handles idempotency to safely process duplicate Pub/Sub messages.
"""

from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from config.settings import settings
from models.event import Event


class StateManager:

    def __init__(self):
        self.window_size = settings.window_size_sec

        # defaultdict handles unknown locations gracefully —
        # no KeyError if a new location arrives in a Pub/Sub message
        self.state = defaultdict(lambda: {
            "riders": deque(),
            "drivers": deque()
        })

        # event_id → event_time mapping for idempotency
        # cleaned up on the same window cycle to prevent unbounded growth
        self.processed_events = {}

    def process_event(self, event: Event) -> None:

        self._cleanup_processed_events()

        # silently ignore duplicates — Pub/Sub at-least-once delivery
        # means we will occasionally receive the same message twice
        if event.event_id in self.processed_events:
            return

        self.processed_events[event.event_id] = event.event_time

        location_state = self.state[event.location]

        if event.event_type == "rider":
            location_state["riders"].append(event.event_time)
        elif event.event_type == "driver":
            location_state["drivers"].append(event.event_time)

        self._cleanup(location_state)

    def get_counts(self, location: str) -> dict:
        location_state = self.state[location]

        # always clean before reading —
        # FastAPI may call get_counts independently of process_event
        self._cleanup(location_state)

        return {
            "riders": len(location_state["riders"]),
            "drivers": len(location_state["drivers"])
        }

    def _cleanup(self, location_state) -> None:
        threshold = datetime.now(timezone.utc) - timedelta(seconds=self.window_size)

        while location_state["riders"] and location_state["riders"][0] < threshold:
            location_state["riders"].popleft()

        while location_state["drivers"] and location_state["drivers"][0] < threshold:
            location_state["drivers"].popleft()

    def _cleanup_processed_events(self) -> None:
        threshold = datetime.now(timezone.utc) - timedelta(seconds=self.window_size)

        old_ids = [
            event_id for event_id, ts in self.processed_events.items()
            if ts < threshold
        ]

        for event_id in old_ids:
            del self.processed_events[event_id]
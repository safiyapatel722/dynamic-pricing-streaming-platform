from collections import deque
from datetime import datetime, timedelta, timezone
from config.settings import CONFIG
from models.event import Event


class StateManager:

    def __init__(self):
        self.window_size = CONFIG["window_size_sec"]

        # state per location
        self.state = {
            location: {
                "riders": deque(),
                "drivers": deque()
            }
            for location in CONFIG["locations"]
        }

        # idempotency storage (event_id -> timestamp)
        self.processed_events = {}

    def process_event(self, event: Event):

        # Step 1: cleanup old processed IDs
        self._cleanup_processed_events()

        # Step 2: idempotency check
        if event.event_id in self.processed_events:
            return

        # Step 3: mark event as processed
        self.processed_events[event.event_id] = event.event_time

        # Step 4: update state
        location_state = self.state[event.location]

        if event.event_type == "rider":
            location_state["riders"].append(event.event_time)

        elif event.event_type == "driver":
            location_state["drivers"].append(event.event_time)

        # Step 5: cleanup windowed events
        self._cleanup(location_state)

    def _cleanup(self, location_state):

        current_time = datetime.now(timezone.utc)  # ✅ FIXED
        threshold = current_time - timedelta(seconds=self.window_size)

        # clean riders
        while location_state["riders"] and location_state["riders"][0] < threshold:
            location_state["riders"].popleft()

        # clean drivers
        while location_state["drivers"] and location_state["drivers"][0] < threshold:
            location_state["drivers"].popleft()

    def _cleanup_processed_events(self):

        current_time = datetime.now(timezone.utc)  # ✅ FIXED
        threshold = current_time - timedelta(seconds=self.window_size)

        # Find all processed event IDs older than the window and remove them
        old_ids = [
            event_id
            for event_id, ts in self.processed_events.items()
            if ts < threshold
        ]

        for event_id in old_ids:
            del self.processed_events[event_id]

    def get_counts(self, location: str):

        location_state = self.state[location]

        return {
            "riders": len(location_state["riders"]),
            "drivers": len(location_state["drivers"])
        }
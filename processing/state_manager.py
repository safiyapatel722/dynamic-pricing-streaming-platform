from collections import deque
from datetime import datetime, timedelta
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

    def process_event(self, event: Event):
        """
        Process incoming event and update state
        """

        location_state = self.state[event.location]

        if event.event_type == "rider":
            location_state["riders"].append(event.event_time)

        elif event.event_type == "driver":
            location_state["drivers"].append(event.event_time)

        # clean old events
        self._cleanup(location_state)

    def _cleanup(self, location_state):
        """
        Remove events older than window
        """

        current_time = datetime.utcnow()
        threshold = current_time - timedelta(seconds=self.window_size)

        # clean riders
        while location_state["riders"] and location_state["riders"][0] < threshold:
            location_state["riders"].popleft()

        # clean drivers
        while location_state["drivers"] and location_state["drivers"][0] < threshold:
            location_state["drivers"].popleft()

    def get_counts(self, location: str):
        """
        Get current demand/supply
        """

        location_state = self.state[location]

        return {
            "riders": len(location_state["riders"]),
            "drivers": len(location_state["drivers"])
        }
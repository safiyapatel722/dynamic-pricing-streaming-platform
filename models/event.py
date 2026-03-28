from dataclasses import dataclass
from datetime import datetime, timezone
event_time=datetime.now(timezone.utc)
import uuid


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str   # "rider" or "driver"
    location: str
    event_time: datetime

    @staticmethod
    def create(event_type: str, location: str):
        return Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            location=location,
            event_time=datetime.now(timezone.utc)
        )
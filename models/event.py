"""
Defines the immutable Event model that flows through the entire pipeline.
Frozen dataclass ensures events cannot be modified after creation.
"""

from dataclasses import dataclass, field
from datetime import datetime
import uuid


@dataclass(frozen=True)
class Event:
    event_type: str       # "rider" or "driver"
    location: str
    event_time: datetime  # explicit — consumer reconstructs with original timestamp
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict:
        """Serialize to dict for Pub/Sub publishing. datetime → isoformat for JSON compatibility."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "location": self.location,
            "event_time": self.event_time.isoformat()
        }
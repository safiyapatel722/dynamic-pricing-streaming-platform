"""
utils/serializer.py

Centralizes all message serialization/deserialization for the pipeline.
Single place to update if message format ever changes.
"""

import json
from models.event import Event


def serialize(event: Event) -> bytes:
    """
    Serialize Event to bytes for Pub/Sub publishing.
    Event → dict → JSON string → bytes
    """
    return json.dumps(event.to_dict()).encode("utf-8")


def deserialize(data: bytes) -> dict:
    """
    Deserialize raw Pub/Sub message bytes back to a dict.
    bytes → JSON string → dict
    """
    return json.loads(data.decode("utf-8"))
"""
utils/redis_client.py

Singleton Redis connection shared across consumer and API.
Locally connects to Redis on localhost.
In production points to GCP Memorystore via settings.
"""

import redis
import json
from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)


# singleton — created once, shared across all imports
_redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    decode_responses=True  # returns str instead of bytes
)


def set_surge(location: str, surge: float, riders: int, drivers: int) -> None:
    """Write surge data for a location to Redis."""
    key = f"surge:{location}"
    value = json.dumps({
        "surge_multiplier": surge,
        "riders": riders,
        "drivers": drivers
    })
    # expire after 2x window size — stale data auto-clears
    _redis_client.set(key, value, ex=settings.window_size_sec * 2)
    logger.info(f"Redis write: {key} = {value}")


def get_surge(location: str) -> dict | None:
    """
    Read surge data for a location from Redis.
    Returns None if no data exists yet for that location.
    """
    key = f"surge:{location}"
    value = _redis_client.get(key)

    if value is None:
        return None

    return json.loads(value)
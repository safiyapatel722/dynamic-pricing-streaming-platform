from processing.state_manager import StateManager
from models.event import Event
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import pytest


@pytest.fixture
def state_manager():
    return StateManager()


@pytest.fixture
def rider_event():
    return Event(
        event_type="rider",
        location="Baner",
        event_time=datetime.now(timezone.utc)
    )


# test 1 — normal event increases count
def test_normal_event_increases_count(state_manager, rider_event):
    state_manager.process_event(rider_event)
    counts = state_manager.get_counts("Baner")
    assert counts["riders"] == 1
    assert counts["drivers"] == 0


# test 2 — duplicate event ignored
def test_duplicate_event_ignored(state_manager, rider_event):
    state_manager.process_event(rider_event)
    state_manager.process_event(rider_event)  # same event_id
    counts = state_manager.get_counts("Baner")
    assert counts["riders"] == 1  # not 2


# test 3 — event outside window discarded
def test_event_outside_window_discarded(state_manager, rider_event):
    state_manager.process_event(rider_event)

    # mock time 120 seconds into future — event is now outside 60s window
    future_time = datetime.now(timezone.utc) + timedelta(seconds=120)
    with patch('processing.state_manager.datetime') as mock_dt:
        mock_dt.now.return_value = future_time
        counts = state_manager.get_counts("Baner")

    assert counts["riders"] == 0
    assert counts["drivers"] == 0


# test 4 — unknown location doesn't crash
def test_unknown_location_no_crash(state_manager):
    event = Event(
        event_type="rider",
        location="Mumbai",  # not in configured locations
        event_time=datetime.now(timezone.utc)
    )
    state_manager.process_event(event)
    counts = state_manager.get_counts("Mumbai")
    assert counts["riders"] == 1
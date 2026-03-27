import random
import time
from datetime import datetime
from models.event import Event
from config.settings import CONFIG


LOCATIONS = CONFIG["locations"]


def generate_event():
    event_type = random.choice(["rider", "driver"])
    location = random.choice(LOCATIONS)

    return Event.create(event_type, location)


def run_simulation(callback):
    """
    Continuously generate events and send them to system
    """

    while True:
        event = generate_event()

        # send event to system
        callback(event)

        time.sleep(1)  # 1 event per second
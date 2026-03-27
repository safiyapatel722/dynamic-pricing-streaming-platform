from ingestion.simulator import run_simulation
from processing.state_manager import StateManager
from processing.surge_calculator import SurgeCalculator


state_manager = StateManager()
surge_calculator = SurgeCalculator()


def handle_event(event):
    # step 1: update state
    state_manager.process_event(event)

    # step 2: get counts for that location
    counts = state_manager.get_counts(event.location)

    # step 3: calculate surge
    surge = surge_calculator.calculate(
        counts["riders"], counts["drivers"]
    )

    # step 4: print result
    print(
        f"{event.location} → Riders: {counts['riders']} "
        f"Drivers: {counts['drivers']} → Surge: {surge}x"
    )


# start system
run_simulation(handle_event)
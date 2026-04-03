from config.settings import settings


class SurgeCalculator:

    def __init__(self):
        self.surge_cap = settings.surge_cap

    def calculate(self, riders: int, drivers: int) -> float:
        """
        Calculate surge multiplier based on demand and supply.
        """

        # Step 1: avoid division by zero
        effective_supply = max(drivers, 1)

        # Step 2: raw surge
        surge = riders / effective_supply

        # Step 3: enforce minimum surge
        surge = max(1, surge)

        # Step 4: cap surge
        surge = min(self.surge_cap, surge)

        # Step 5: round for readability
        return round(surge, 2)
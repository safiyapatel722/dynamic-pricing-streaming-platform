from processing.surge_calculator import SurgeCalculator
import pytest

@pytest.fixture
def calculator():
    return SurgeCalculator()


# 1. more riders than drivers     → surge > 1x

def test_surge_with_more_riders(calculator):
   
    result = calculator.calculate(riders=10, drivers=5)
    assert result == 2.0

#2. more drivers than riders     → surge = 1x (floor)
def test_surge_with_more_drivers(calculator):
   
    result = calculator.calculate(riders=4, drivers=8)
    assert result == 1.0

#3. zero drivers                 → no division error, surge capped
def test_zero_drivers(calculator):
   
    result = calculator.calculate(riders= 10, drivers=0)
    assert result == 3.0

#4. zero riders                  → surge = 1x (floor)
def test_zero_riders(calculator):
    
    result = calculator.calculate(riders=0,drivers=8)
    assert result == 1.0

#5. equal riders and drivers     → surge = 1x
def test_equal_drivers_riders(calculator):
    
    result = calculator.calculate(riders=10,drivers=10)
    assert result == 1.0

#6. extreme imbalance            → surge = 3x (cap enforced)
def test_riders_imbalance(calculator):
    
    result = calculator.calculate(riders=100,drivers=1)
    assert result == 3.0
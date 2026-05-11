import threading
import time
import random

class Intersection:
    def __init__(self):
        
        self.locks = {
            'north_south': threading.Lock(),
            'east_west': threading.Lock()
        }

def ordered_locks(a: threading.Lock, b: threading.Lock):
    """Return (first, second) in a consistent global order."""
    return (a, b) if id(a) < id(b) else (b, a)

class Car(threading.Thread):
    def __init__(self, car_id, intersection, direction):
        threading.Thread.__init__(self)
        self.car_id = car_id
        self.intersection = intersection
        self.direction = direction
        
    def run(self):
        
        time.sleep(random.uniform(0.1, 1.0))
        
        
        if self.direction in ['North', 'South']:
            first = self.intersection.locks['north_south']
            second = self.intersection.locks['east_west']
        else:
            first = self.intersection.locks['east_west']
            second = self.intersection.locks['north_south']

        
        lock1, lock2 = ordered_locks(first, second)

        print(f"Car {self.car_id} from {self.direction} approaching intersection")

        
        with lock1:
            time.sleep(random.uniform(0.05, 0.2))  
            with lock2:
                
                print(f"Car {self.car_id} from {self.direction} crossing intersection")
                time.sleep(random.uniform(0.3, 0.7))

        print(f"Car {self.car_id} from {self.direction} has passed through")

def simulate_traffic():
    
    intersection = Intersection()
    
    
    directions = ['North', 'South', 'East', 'West']
    
    
    cars = []
    for i in range(10):
        direction = random.choice(directions)
        car = Car(i, intersection, direction)
        cars.append(car)
        car.start()
    
    
    for car in cars:
        car.join()
        
    print("terminated")

if __name__ == "__main__":
    simulate_traffic()

import threading
import time
import random

class Intersection:
    def __init__(self):
        
        self.locks = {
            'north_south': threading.Lock(),
            'east_west': threading.Lock()
        }

class Car(threading.Thread):
    def __init__(self, car_id, intersection, direction):
        super().__init__()
        self.car_id = car_id
        self.intersection = intersection
        self.direction = direction

    def run(self):
        
        time.sleep(random.uniform(0.1, 1.0))

        
        ns_lock = self.intersection.locks['north_south']
        ew_lock = self.intersection.locks['east_west']

        
        
        
        first_lock, second_lock = sorted([ns_lock, ew_lock], key=id)

        print(f"Car {self.car_id} from {self.direction} approaching intersection")

        
        with first_lock:
            
            time.sleep(random.uniform(0.05, 0.2))
            with second_lock:
                
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

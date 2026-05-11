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
        threading.Thread.__init__(self)
        self.car_id = car_id
        self.intersection = intersection
        self.direction = direction

    def run(self):
        time.sleep(random.uniform(0.1, 1.0))
        
        print(f"Car {self.car_id} from {self.direction} approaching intersection")
        
        
        ns_lock = self.intersection.locks['north_south']
        ew_lock = self.intersection.locks['east_west']
        
        acquired_ns = False
        acquired_ew = False
        
        try:
            if not ns_lock.acquire(timeout=1.0):
                print(f"Car {self.car_id} timed out on north_south lock")
                return
            acquired_ns = True
            
            if not ew_lock.acquire(timeout=1.0):
                print(f"Car {self.car_id} timed out on east_west lock")
                return
            acquired_ew = True
            
            print(f"Car {self.car_id} from {self.direction} crossing intersection")
            time.sleep(random.uniform(0.3, 0.7))
            
        finally:
            if acquired_ew:
                ew_lock.release()
            if acquired_ns:
                ns_lock.release()
        
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

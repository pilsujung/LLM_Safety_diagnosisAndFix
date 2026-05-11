import threading
import time
import random

class Intersection:
    def __init__(self):
        
        self.locks = {
            'north_south': threading.Lock(),
            'east_west': threading.Lock()
        }
        
        
        self.lock_order = ['north_south', 'east_west']

class Car(threading.Thread):
    def __init__(self, car_id, intersection, direction):
        threading.Thread.__init__(self)
        self.car_id = car_id
        self.intersection = intersection
        self.direction = direction

    def run(self):
        
        time.sleep(random.uniform(0.1, 1.0))

        print(f"Car {self.car_id} from {self.direction} approaching intersection")

        
        if self.direction in ['North', 'South']:
            first_lock_name = 'north_south'
            second_lock_name = 'east_west'
        else:  
            first_lock_name = 'north_south'  
            second_lock_name = 'east_west'

        first_lock = self.intersection.locks[first_lock_name]
        second_lock = self.intersection.locks[second_lock_name]

        print(f"Car {self.car_id} from {self.direction}: attempting to acquire {first_lock_name}")
        
        
        first_lock.acquire()
        try:
            print(f"Car {self.car_id} from {self.direction}: acquired {first_lock_name}, waiting for {second_lock_name}")
            time.sleep(random.uniform(0.05, 0.2))  
            
            second_lock.acquire()
            try:
                
                print(f"Car {self.car_id} from {self.direction} crossing intersection")
                time.sleep(random.uniform(0.3, 0.7))
            finally:
                second_lock.release()
        finally:
            first_lock.release()

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

    print("Simulation terminated")

if __name__ == "__main__":
    simulate_traffic()

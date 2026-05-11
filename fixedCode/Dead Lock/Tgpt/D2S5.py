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
        
        
        if self.direction in ['North', 'South']:
            lock_a = self.intersection.locks['north_south']
            lock_b = self.intersection.locks['east_west']
        else:
            lock_a = self.intersection.locks['east_west']
            lock_b = self.intersection.locks['north_south']

        
        
        if id(lock_a) < id(lock_b):
            first_lock, second_lock = lock_a, lock_b
        else:
            first_lock, second_lock = lock_b, lock_a
        
        print(f"Car {self.car_id} from {self.direction} approaching intersection")
        
        
        first_lock.acquire()
        time.sleep(random.uniform(0.05, 0.2))  
        
        try:
            second_lock.acquire()
            
            
            print(f"Car {self.car_id} from {self.direction} crossing intersection")
            time.sleep(random.uniform(0.3, 0.7))
        
        finally:
            
            second_lock.release()
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
        
    print("terminated")

if __name__ == "__main__":
    simulate_traffic()

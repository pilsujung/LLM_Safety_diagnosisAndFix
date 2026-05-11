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
        
        
        
        
        first_lock = self.intersection.locks['north_south']
        second_lock = self.intersection.locks['east_west']
        
        
        print(f"Car {self.car_id} from {self.direction} approaching intersection")
        
        
        first_lock.acquire()
        time.sleep(random.uniform(0.05, 0.2))  
        
        try:
            second_lock.acquire()
            
            
            print(f"Car {self.car_id} from {self.direction} crossing intersection")
            time.sleep(random.uniform(0.3, 0.7))
        
        finally:
            
            first_lock.release()
            second_lock.release()
        
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
        
    print(f"terminated")

if __name__ == "__main__":
    simulate_traffic()

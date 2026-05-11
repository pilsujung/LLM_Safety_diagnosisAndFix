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
         
        
        
        lock1 = self.intersection.locks['east_west']  
        lock2 = self.intersection.locks['north_south']
        
        
        lock1.acquire() 
        time.sleep(random.uniform(0.05, 0.2))  
         
        try: 
            lock2.acquire() 
             
            
            print(f"Car {self.car_id} from {self.direction} crossing intersection") 
            time.sleep(random.uniform(0.3, 0.7)) 
         
        finally: 
            
            lock2.release()
            lock1.release() 
         
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
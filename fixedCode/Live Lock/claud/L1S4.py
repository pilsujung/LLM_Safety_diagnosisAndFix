import threading
import time
import random

class Resource:
    def __init__(self, name, order_id):
        self.name = name
        self.order_id = order_id
        self.lock = threading.Lock()
        self.owner = None
    
    def acquire(self, owner):
        """Try to acquire the resource for the given owner"""
        result = self.lock.acquire(blocking=False)
        if result:
            self.owner = owner
            print(f"{time.time():.2f}: {owner} acquired {self.name}")
        return result
    
    def release(self):
        """Release the resource"""
        previous_owner = self.owner
        self.owner = None
        self.lock.release()
        print(f"{time.time():.2f}: {previous_owner} released {self.name}")


class Worker(threading.Thread):
    def __init__(self, name, first_resource, second_resource):
        super().__init__(name=name)

        self.resources = sorted([first_resource, second_resource], key=lambda r: r.order_id)
        self.first_resource = self.resources[0]
        self.second_resource = self.resources[1]
        self.active = True
        self.last_progress = time.time()
    
    def run(self):
        while self.active:

            if self.first_resource.acquire(self.name):
                try:

                    time.sleep(0.1)
                    

                    print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")
                    

                    if self.second_resource.lock.acquire(timeout=0.5):
                        try:
                            self.second_resource.owner = self.name
                            print(f"{time.time():.2f}: {self.name} acquired {self.second_resource.name}")
                            

                            self.last_progress = time.time()
                            print(f"{time.time():.2f}: {self.name} has both resources!")
                            

                            time.sleep(0.2)
                            
                        finally:

                            self.second_resource.release()
                    else:
                        print(f"{time.time():.2f}: {self.name} timed out waiting for {self.second_resource.name}")
                    
                finally:

                    self.first_resource.release()
            

            time.sleep(random.uniform(0.1, 0.3))


def detect_livelock(workers, timeout=10):
    """Monitor worker threads to detect livelock condition"""
    start_time = time.time()
    
    while all(worker.is_alive() for worker in workers):

        current_time = time.time()
        

        for worker in workers:
            if current_time - worker.last_progress < timeout:
                print(f"{time.time():.2f}: {worker.name} is making progress")
        
        stuck_workers = [w for w in workers if current_time - w.last_progress > timeout]
        
        if len(stuck_workers) == len(workers) and current_time - start_time > timeout:
            print(f"\n{'='*60}")
            print(f"POTENTIAL ISSUE: Workers haven't made progress recently.")
            print(f"Elapsed time: {current_time - start_time:.2f} seconds")
            print(f"{'='*60}\n")
            

            time.sleep(2)
            

            for worker in workers:
                worker.active = False
            break
        

        if current_time - start_time > 20:
            print(f"\nSimulation complete after {current_time - start_time:.2f} seconds")
            for worker in workers:
                worker.active = False
            break
            
        time.sleep(1)


def main():

    resource_x = Resource("Resource X", order_id=1)
    resource_y = Resource("Resource Y", order_id=2)
    

    thread_a = Worker("Thread A", resource_x, resource_y)
    thread_b = Worker("Thread B", resource_y, resource_x)
    

    print("Starting workers...")
    thread_a.start()
    thread_b.start()
    

    detector = threading.Thread(target=detect_livelock, args=([thread_a, thread_b],))
    detector.start()
    

    detector.join()
    thread_a.join(timeout=1)
    thread_b.join(timeout=1)
    
    print("Simulation completed.")


if __name__ == "__main__":
    main()
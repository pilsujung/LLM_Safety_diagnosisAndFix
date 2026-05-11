import threading
import time
import random

class Resource:
    def __init__(self, name, resource_id):
        self.name = name
        self.resource_id = resource_id
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
    def __init__(self, name, resource1, resource2):
        super().__init__(name=name)

        if resource1.resource_id < resource2.resource_id:
            self.first_resource = resource1
            self.second_resource = resource2
        else:
            self.first_resource = resource2
            self.second_resource = resource1
        self.active = True
        self.last_progress = time.time()
    
    def run(self):
        while self.active:

            if self.acquire_both_resources():

                self.last_progress = time.time()
                print(f"{time.time():.2f}: {self.name} has both resources!")
                

                time.sleep(0.2)
                

                self.second_resource.release()
                self.first_resource.release()
            

            wait_time = random.uniform(0.1, 0.5)
            time.sleep(wait_time)
    
    def acquire_both_resources(self):
        """
        Step 3 Implementation: Try to acquire both resources atomically
        If we can't get both, release any we've acquired and return False
        """

        if not self.first_resource.acquire(self.name):
            return False
        

        print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")
        

        acquired_second = self.second_resource.lock.acquire(timeout=0.5)
        
        if acquired_second:
            self.second_resource.owner = self.name
            print(f"{time.time():.2f}: {self.name} acquired {self.second_resource.name}")
            return True
        else:

            print(f"{time.time():.2f}: {self.name} failed to acquire {self.second_resource.name}, releasing {self.first_resource.name}")
            self.first_resource.release()
            return False


def detect_livelock(workers, timeout=10):
    """Monitor worker threads to detect livelock condition"""
    start_time = time.time()
    
    while all(worker.is_alive() for worker in workers):

        current_time = time.time()
        stuck_workers = [w for w in workers if current_time - w.last_progress > timeout]
        
        if len(stuck_workers) == len(workers) and current_time - start_time > timeout:
            print(f"\n{'='*60}")
            print(f"LIVELOCK DETECTED! All workers are stuck waiting for resources.")
            print(f"Elapsed time: {current_time - start_time:.2f} seconds")
            print(f"{'='*60}\n")
            

            for worker in workers:
                worker.active = False
            break
        
        time.sleep(1)
    

    if time.time() - start_time > 15:
        print("Terminating workers after successful execution period...")
        for worker in workers:
            worker.active = False


def main():

    resource_x = Resource("Resource X", 1)
    resource_y = Resource("Resource Y", 2)
    

    thread_a = Worker("Thread A", resource_x, resource_y)
    thread_b = Worker("Thread B", resource_y, resource_x)
    

    print("Starting workers with livelock prevention...")
    print("Both threads will acquire resources in ID order: X(1) then Y(2)")
    print("="*60)
    
    thread_a.start()
    thread_b.start()
    

    detector = threading.Thread(target=detect_livelock, args=([thread_a, thread_b],))
    detector.start()
    

    detector.join()
    thread_a.join(timeout=1)
    thread_b.join(timeout=1)
    
    print("Simulation completed without livelock!")


if __name__ == "__main__":
    main()
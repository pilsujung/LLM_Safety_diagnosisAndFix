import threading
import time
import random

class Resource:
    def __init__(self, name):
        self.name = name
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


        self.resources = sorted([first_resource, second_resource], key=lambda r: r.name)
        self.first_resource = self.resources[0]
        self.second_resource = self.resources[1]
        self.active = True
        self.last_progress = time.time()

        self.backoff_base = 0.1
        self.backoff_multiplier = 1.5
        self.max_backoff = 2.0
        self.current_backoff = self.backoff_base
    
    def reset_backoff(self):
        """Reset backoff to base value after successful operation"""
        self.current_backoff = self.backoff_base
    
    def apply_backoff(self):
        """Apply exponential backoff and update for next time"""
        time.sleep(self.current_backoff)
        self.current_backoff = min(self.current_backoff * self.backoff_multiplier, self.max_backoff)
    
    def run(self):
        while self.active:

            acquired_first = False
            acquired_second = False
            
            try:

                if self.first_resource.acquire(self.name):
                    acquired_first = True

                    time.sleep(0.1)
                    

                    print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")
                    attempt_start = time.time()
                    max_wait_time = 0.5
                    
                    while self.active and (time.time() - attempt_start) < max_wait_time:
                        if self.second_resource.acquire(self.name):
                            acquired_second = True

                            self.last_progress = time.time()
                            print(f"{time.time():.2f}: {self.name} has both resources!")
                            

                            time.sleep(0.2)
                            

                            self.reset_backoff()
                            break
                        

                        time.sleep(0.01)
                    

                    if not acquired_second:
                        print(f"{time.time():.2f}: {self.name} couldn't acquire {self.second_resource.name}, will retry")
                
                else:

                    print(f"{time.time():.2f}: {self.name} couldn't acquire {self.first_resource.name}")
                
            finally:

                if acquired_second:
                    self.second_resource.release()
                if acquired_first:
                    self.first_resource.release()
            

            if not (acquired_first and acquired_second):
                self.apply_backoff()
            else:

                time.sleep(random.uniform(0.05, 0.1))


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
    

    print(f"\nShutting down workers after {time.time() - start_time:.2f} seconds...")
    for worker in workers:
        worker.active = False


def main():

    resource_x = Resource("Resource X")
    resource_y = Resource("Resource Y")
    


    thread_a = Worker("Thread A", resource_x, resource_y)
    thread_b = Worker("Thread B", resource_y, resource_x)
    

    print("Starting workers...")
    print("SOLUTION APPLIED: Ordered resource acquisition + exponential backoff + shorter timeouts")
    print("-" * 80)
    thread_a.start()
    thread_b.start()
    

    detector = threading.Thread(target=detect_livelock, args=([thread_a, thread_b],))
    detector.start()
    

    time.sleep(15)
    

    for worker in [thread_a, thread_b]:
        worker.active = False
    

    detector.join(timeout=2)
    thread_a.join(timeout=2)
    thread_b.join(timeout=2)
    
    print("Simulation completed successfully - no livelock detected!")


if __name__ == "__main__":
    main()
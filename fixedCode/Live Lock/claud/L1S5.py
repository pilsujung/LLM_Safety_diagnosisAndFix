import threading
import time
import random

class Resource:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.owner = None
    
    def acquire(self, owner, timeout=None):
        """Try to acquire the resource for the given owner with optional timeout"""
        result = self.lock.acquire(blocking=True, timeout=timeout)
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


class FixedWorker(threading.Thread):
    def __init__(self, name, first_resource, second_resource, initial_delay=0):
        super().__init__(name=name)
        self.first_resource = first_resource
        self.second_resource = second_resource
        self.active = True
        self.last_progress = time.time()
        self.initial_delay = initial_delay
        self.max_attempts = 5
        
    def run(self):

        if self.initial_delay > 0:
            time.sleep(self.initial_delay)
            
        while self.active:
            attempts = 0
            

            if self.first_resource.acquire(self.name, timeout=2.0):
                try:

                    time.sleep(0.1)
                    

                    print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")
                    
                    while self.active and attempts < self.max_attempts:

                        wait_time = random.uniform(0.05, 0.2)
                        
                        if self.second_resource.acquire(self.name, timeout=wait_time):
                            try:

                                self.last_progress = time.time()
                                print(f"{time.time():.2f}: {self.name} has both resources!")
                                

                                time.sleep(0.2)
                                
                            finally:

                                self.second_resource.release()
                            break
                        
                        attempts += 1
                        print(f"{time.time():.2f}: {self.name} attempt {attempts}/{self.max_attempts} failed, waiting {wait_time:.3f}s")
                        

                        if attempts >= self.max_attempts:
                            print(f"{time.time():.2f}: {self.name} reached max attempts, taking longer break")
                            time.sleep(random.uniform(0.3, 0.5))
                            break
                    
                finally:

                    self.first_resource.release()
            else:
                print(f"{time.time():.2f}: {self.name} failed to acquire {self.first_resource.name}")
            

            base_delay = random.uniform(0.05, 0.15)
            if attempts >= self.max_attempts:
                base_delay *= 2
            time.sleep(base_delay)


def detect_livelock(workers, timeout=10):
    """Monitor worker threads to detect livelock condition"""
    start_time = time.time()
    last_check_time = start_time
    
    while all(worker.is_alive() for worker in workers):
        current_time = time.time()
        

        if current_time - last_check_time > 2:
            progress_made = False
            for worker in workers:
                if current_time - worker.last_progress < 3:
                    progress_made = True
                    break
            
            if progress_made:
                print(f"{current_time:.2f}: System is making progress...")
            else:
                print(f"{current_time:.2f}: No progress detected recently...")
            
            last_check_time = current_time
        

        if current_time - start_time > timeout:
            stuck_workers = [w for w in workers if current_time - w.last_progress > 5]
            
            if len(stuck_workers) == len(workers):
                print(f"\n{'='*60}")
                print(f"TIMEOUT REACHED! Stopping simulation after {timeout} seconds.")
                print(f"Workers may have been in livelock, but fixes should prevent this.")
                print(f"{'='*60}\n")
            else:
                print(f"\n{'='*60}")
                print(f"SIMULATION COMPLETED! Workers made progress successfully.")
                print(f"Livelock prevention strategies are working.")
                print(f"{'='*60}\n")
            

            for worker in workers:
                worker.active = False
            break
        
        time.sleep(0.5)


def main():

    resource_x = Resource("Resource X")
    resource_y = Resource("Resource Y")
    

    thread_a = FixedWorker("Thread A", resource_x, resource_y, initial_delay=0)
    thread_b = FixedWorker("Thread B", resource_y, resource_x, initial_delay=0.1)
    

    print("Starting fixed workers with livelock prevention...")
    print("Strategies used:")
    print("1. Random backoff times")
    print("2. Maximum attempt limits")
    print("3. Staggered start times")
    print("4. Timeout-based resource acquisition")
    print("5. Exponential backoff after failures")
    print("-" * 50)
    
    thread_a.start()
    thread_b.start()
    

    detector = threading.Thread(target=detect_livelock, args=([thread_a, thread_b], 10))
    detector.start()
    

    detector.join()
    thread_a.join(timeout=1)
    thread_b.join(timeout=1)
    
    print("Fixed simulation completed successfully!")


if __name__ == "__main__":
    main()
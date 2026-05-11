import threading
import time
import random

class Resource:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.owner = None
    
    def acquire(self, owner):
        """Try to acquire the resource for the given owner (non-blocking)"""
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

        self.r_low, self.r_high = sorted([first_resource, second_resource], key=lambda r: r.name)
        self.active = True
        self.last_progress = time.time()
    
    def run(self):
        while self.active:

            if self.r_low.acquire(self.name):
                try:

                    time.sleep(0.05)
                    

                    print(f"{time.time():.2f}: {self.name} is trying to acquire {self.r_high.name}")
                    attempt_start = time.time()
                    
                    while self.active:
                        if self.r_high.acquire(self.name):
                            try:

                                self.last_progress = time.time()
                                print(f"{time.time():.2f}: {self.name} has both resources!")
                                

                                time.sleep(0.2)
                            finally:
                                self.r_high.release()
                            break
                        

                        time.sleep(0.05)
                        

                        if time.time() - attempt_start > 1.0:
                            print(f"{time.time():.2f}: {self.name} giving up on {self.r_high.name} temporarily")
                            break
                finally:
                    self.r_low.release()
            

            time.sleep(random.uniform(0.05, 0.1))


def detect_livelock(workers, timeout=5):
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


def main():

    resource_x = Resource("Resource X")
    resource_y = Resource("Resource Y")
    

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

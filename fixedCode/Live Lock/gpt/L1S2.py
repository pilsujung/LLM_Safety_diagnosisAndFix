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
        self.first_resource = first_resource
        self.second_resource = second_resource
        self.active = True
        self.last_progress = time.time()

        seed = hash((name, time.time_ns()))
        self.rng = random.Random(seed)

    def _jitter(self, lo_ms=50, hi_ms=200):
        """Random backoff like the C++ example (50–200ms)."""
        wait_ms = self.rng.randint(lo_ms, hi_ms)
        print(f"{time.time():.2f}: {self.name} waiting {wait_ms}ms")
        time.sleep(wait_ms / 1000.0)

    def run(self):
        while self.active:


            self._jitter(50, 200)


            if not self.active:
                break

            if self.first_resource.acquire(self.name):
                try:

                    time.sleep(0.05)


                    print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")


                    backoff_ms = 50
                    attempt_count = 0
                    attempt_start = time.time()

                    while self.active:
                        if self.second_resource.acquire(self.name):
                            try:
                                self.last_progress = time.time()
                                print(f"{time.time():.2f}: {self.name} has both resources!")
                                time.sleep(0.2)
                            finally:
                                self.second_resource.release()
                            break



                        jitter_hi = min(400, backoff_ms * 2)
                        self._jitter(50, jitter_hi)
                        backoff_ms = min(400, backoff_ms * 2)
                        attempt_count += 1


                        if time.time() - attempt_start > 1.0:
                            print(f"{time.time():.2f}: {self.name} temporarily giving up on {self.second_resource.name}")

                            self._jitter(80, 240)
                            break

                finally:
                    self.first_resource.release()


            self._jitter(60, 180)


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
    


    time.sleep(8)
    thread_a.active = False
    thread_b.active = False

    detector.join()
    thread_a.join(timeout=1)
    thread_b.join(timeout=1)
    
    print("Simulation completed.")


if __name__ == "__main__":
    main()

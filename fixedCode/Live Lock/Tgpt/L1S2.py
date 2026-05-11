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
    
    def run(self):

        time.sleep(random.uniform(0.01, 0.05))

        while self.active:

            if not self.first_resource.acquire(self.name):

                time.sleep(random.uniform(0.01, 0.05))
                continue

            got_both = False
            try:

                time.sleep(0.05)




                backoff_base = 0.05
                backoff_max = 0.30

                while self.active and not got_both:
                    if self.second_resource.acquire(self.name):
                        try:
                            self.last_progress = time.time()
                            print(f"{time.time():.2f}: {self.name} has both resources!")
                            time.sleep(0.2)
                            got_both = True
                        finally:
                            self.second_resource.release()
                        break



                    wait_time = random.uniform(backoff_base, backoff_max)
                    print(f"{time.time():.2f}: {self.name} backing off {int(wait_time*1000)}ms from {self.second_resource.name} and releasing {self.first_resource.name}")
                    self.first_resource.release()


                    time.sleep(wait_time)


                    if not self.active:
                        break
                    if not self.first_resource.acquire(self.name):

                        time.sleep(random.uniform(0.02, 0.08))
                        break


                    backoff_base = min(backoff_base * 1.2, backoff_max)
                    backoff_max = min(backoff_max * 1.2, 0.6)

            finally:

                if self.first_resource.owner == self.name:
                    self.first_resource.release()


            time.sleep(random.uniform(0.03, 0.12))


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
    

    time.sleep(6)
    thread_a.active = False
    thread_b.active = False


    detector.join()
    thread_a.join(timeout=1)
    thread_b.join(timeout=1)
    
    print("Simulation completed.")


if __name__ == "__main__":
    main()

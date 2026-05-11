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
    def __init__(self, name, resource1, resource2):
        super().__init__(name=name)

        if resource1.name < resource2.name:
            self.first_resource = resource1
            self.second_resource = resource2
        else:
            self.first_resource = resource2
            self.second_resource = resource1
        self.active = True
        self.last_progress = time.time()

    def acquire_both(self):
        """Acquire both resources in order, blocking until successful"""
        while self.active:
            acquired_first = self.first_resource.lock.acquire(timeout=0.1)
            if not acquired_first:
                continue
            acquired_second = self.second_resource.lock.acquire(timeout=0.1)
            if acquired_second:
                self.first_resource.owner = self.name
                self.second_resource.owner = self.name
                print(f"{time.time():.2f}: {self.name} acquired both resources")
                return True
            else:
                self.first_resource.lock.release()
                time.sleep(random.uniform(0.01, 0.05))
        return False

    def release_both(self):
        self.second_resource.owner = None
        self.second_resource.lock.release()
        print(f"{time.time():.2f}: {self.name} released {self.second_resource.name}")

        self.first_resource.owner = None
        self.first_resource.lock.release()
        print(f"{time.time():.2f}: {self.name} released {self.first_resource.name}")

    def run(self):
        while self.active:
            if self.acquire_both():
                try:
                    self.last_progress = time.time()

                    time.sleep(0.2)
                finally:
                    self.release_both()

                    time.sleep(random.uniform(0.05, 0.1))

def detect_livelock(workers, timeout=5):
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

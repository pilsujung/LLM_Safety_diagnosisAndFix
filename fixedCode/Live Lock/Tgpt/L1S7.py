import threading
import time
import random

class Resource:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.owner = None

    def acquire(self, owner, blocking=True, timeout=None):
        """
        Try to acquire the resource for the given owner.
        If blocking is True and timeout is not None, will wait up to timeout seconds.
        """
        if blocking and timeout is not None:
            result = self.lock.acquire(timeout=timeout)
        else:
            result = self.lock.acquire(blocking=blocking)

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

        res_a, res_b = sorted([self.first_resource, self.second_resource], key=lambda r: r.name)

        while self.active:

            if not self.active:
                break
            got_a = res_a.acquire(self.name, blocking=True, timeout=0.5)
            if not got_a:

                time.sleep(random.uniform(0.02, 0.08))
                continue

            try:

                print(f"{time.time():.2f}: {self.name} is trying to acquire {res_b.name}")
                got_b = res_b.acquire(self.name, blocking=True, timeout=0.5)

                if not got_b:

                    print(f"{time.time():.2f}: {self.name} couldn’t get {res_b.name}, releasing {res_a.name} and retrying")
                    res_a.release()

                    time.sleep(random.uniform(0.05, 0.15))
                    continue

                try:

                    self.last_progress = time.time()
                    print(f"{time.time():.2f}: {self.name} has both resources!")
                    time.sleep(0.2)
                finally:
                    res_b.release()
            finally:

                if res_a.owner == self.name:
                    res_a.release()


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
    thread_a.active = False
    thread_b.active = False
    thread_a.join(timeout=1)
    thread_b.join(timeout=1)

    print("Simulation completed.")


if __name__ == "__main__":
    main()

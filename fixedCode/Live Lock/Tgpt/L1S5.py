import threading
import time
import random


class Resource:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.owner = None

    def acquire(self, owner, timeout=None):
        """Try to acquire the resource for the given owner, with optional timeout."""
        if timeout is None:
            result = self.lock.acquire(blocking=False)
        else:
            result = self.lock.acquire(timeout=timeout)

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
        self._rng = random.Random(hash(name) ^ int(time.time()))

    def run(self):

        time.sleep(self._rng.uniform(0.05, 0.25))

        failed_rounds = 0
        while self.active:

            base_wait = min(0.4, 0.05 * (2 ** min(failed_rounds, 4)))
            jitter = self._rng.uniform(0.0, 0.15)
            time.sleep(base_wait + jitter)


            first_timeout = self._rng.uniform(0.05, 0.2)
            got_first = self.first_resource.acquire(self.name, timeout=first_timeout)
            if not got_first:

                continue

            try:

                time.sleep(self._rng.uniform(0.05, 0.12))

                print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")
                attempt_start = time.time()


                attempts_for_second = 0
                got_both = False
                while self.active:

                    t_out = min(0.35, 0.08 + 0.05 * attempts_for_second) + self._rng.uniform(0.0, 0.1)
                    if self.second_resource.acquire(self.name, timeout=t_out):
                        try:
                            self.last_progress = time.time()
                            print(f"{time.time():.2f}: {self.name} has both resources!")

                            time.sleep(self._rng.uniform(0.15, 0.3))
                            got_both = True
                        finally:
                            self.second_resource.release()
                        break

                    attempts_for_second += 1


                    elapsed = time.time() - attempt_start
                    if attempts_for_second >= 5 or elapsed > 1.0:
                        print(f"{time.time():.2f}: {self.name} backing off from {self.second_resource.name}")
                        break

                if got_both:
                    failed_rounds = 0
                else:
                    failed_rounds += 1

            finally:

                self.first_resource.release()


            time.sleep(self._rng.uniform(0.03, 0.12))


def detect_livelock(workers, timeout=5):
    """Monitor worker threads to detect livelock condition"""
    start_time = time.time()

    while all(worker.is_alive() for worker in workers):
        current_time = time.time()
        stuck_workers = [w for w in workers if current_time - w.last_progress > timeout]

        if len(stuck_workers) == len(workers) and current_time - start_time > timeout:
            print(f"\n{'='*60}")
            print("LIVELOCK DETECTED! All workers are stuck waiting for resources.")
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

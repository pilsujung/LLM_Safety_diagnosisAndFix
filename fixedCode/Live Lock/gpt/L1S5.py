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
        got = self.lock.acquire(blocking=False)
        if got:
            self.owner = owner
            print(f"{time.time():.2f}: {owner} acquired {self.name}")
        return got

    def release(self):
        """Release the resource"""
        prev = self.owner
        self.owner = None
        self.lock.release()
        print(f"{time.time():.2f}: {prev} released {self.name}")


class Worker(threading.Thread):
    def __init__(self, name, first_resource, second_resource, priorities):
        super().__init__(name=name)
        self.first_resource = first_resource
        self.second_resource = second_resource
        self.active = True
        self.last_progress = time.time()
        self.priorities = priorities
        self.priority = priorities[name]

        self.start_jitter = random.uniform(0.0, 0.2)

    def polite_backoff(self):
        """Randomized backoff like the examples (50–250ms)"""
        time.sleep(random.uniform(0.05, 0.25))

    def long_backoff(self):
        """Longer backoff to decisively break symmetry (200–600ms)"""
        time.sleep(random.uniform(0.2, 0.6))

    def run(self):
        time.sleep(self.start_jitter)

        while self.active:

            if self.first_resource.acquire(self.name):
                try:

                    time.sleep(0.05)


                    print(f"{time.time():.2f}: {self.name} is trying to acquire {self.second_resource.name}")

                    attempts = 0
                    attempt_window_start = time.time()

                    while self.active:
                        if self.second_resource.acquire(self.name):
                            try:
                                self.last_progress = time.time()
                                print(f"{time.time():.2f}: {self.name} has both resources!")
                                time.sleep(0.15)
                            finally:
                                self.second_resource.release()
                            break


                        holder = self.second_resource.owner
                        attempts += 1


                        if holder and holder != self.name:
                            my_p = self.priority
                            other_p = self.priorities.get(holder, -1)

                            if my_p > other_p:

                                print(f"{time.time():.2f}: {self.name} has higher priority ({my_p}>{other_p}); staying on {self.first_resource.name}")
                                time.sleep(0.03 + random.random() * 0.05)
                            else:

                                print(f"{time.time():.2f}: {self.name} has lower priority ({my_p}<{other_p}); backing off")

                                self.first_resource.release()
                                self.long_backoff()
                                break
                        else:

                            self.polite_backoff()


                        if attempts >= 5 or (time.time() - attempt_window_start) > 1.0:

                            if random.random() < 0.5:
                                print(f"{time.time():.2f}: {self.name} forcing progress after retries (holding {self.first_resource.name})")

                                time.sleep(0.05 + random.random() * 0.05)
                            else:
                                print(f"{time.time():.2f}: {self.name} yielding after retries (releasing {self.first_resource.name})")
                                self.first_resource.release()
                                self.long_backoff()
                                break

                finally:

                    if self.first_resource.owner == self.name and self.first_resource.lock.locked():
                        self.first_resource.release()


            time.sleep(random.uniform(0.03, 0.08))


def detect_livelock(workers, timeout=5):
    """Monitor worker threads to detect livelock condition"""
    start_time = time.time()

    while all(w.is_alive() for w in workers):
        now = time.time()
        stuck = [w for w in workers if now - w.last_progress > timeout]

        if len(stuck) == len(workers) and now - start_time > timeout:
            print("\n" + "=" * 60)
            print("LIVELOCK DETECTED! All workers look stuck.")
            print(f"Elapsed time: {now - start_time:.2f} seconds")
            print("=" * 60 + "\n")

            for w in workers:
                w.active = False
            break

        time.sleep(1)


def main():

    rx = Resource("Resource X")
    ry = Resource("Resource Y")


    priorities = {
        "Thread A": random.randint(1, 1000),
        "Thread B": random.randint(1, 1000),
    }
    print(f"Priorities: {priorities}")


    ta = Worker("Thread A", rx, ry, priorities)
    tb = Worker("Thread B", ry, rx, priorities)

    print("Starting workers...")
    ta.start()
    tb.start()


    detector = threading.Thread(target=detect_livelock, args=([ta, tb],))
    detector.start()

    detector.join()
    ta.join(timeout=1)
    tb.join(timeout=1)

    print("Simulation completed.")


if __name__ == "__main__":
    main()

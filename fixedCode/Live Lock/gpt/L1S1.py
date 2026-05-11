import threading
import time

class Resource:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.owner = None

    def acquire(self, owner, blocking=True, timeout=None):
        """Acquire with optional timeout; set owner on success."""
        if timeout is None:
            ok = self.lock.acquire(blocking=blocking)
        else:
            ok = self.lock.acquire(timeout=timeout)
        if ok:
            self.owner = owner
            print(f"{time.time():.2f}: {owner} acquired {self.name}")
        return ok

    def release(self):
        prev = self.owner
        self.owner = None
        self.lock.release()
        print(f"{time.time():.2f}: {prev} released {self.name}")


class Worker(threading.Thread):
    def __init__(self, name, first_resource, second_resource):
        super().__init__(name=name)
        self.first_resource = first_resource
        self.second_resource = second_resource
        self.active = True
        self.last_progress = time.time()

    def run(self):

        low, high = sorted([self.first_resource, self.second_resource], key=lambda r: r.name)

        while self.active:

            low.acquire(self.name)
            try:

                time.sleep(0.05)

                high.acquire(self.name)
                try:
                    self.last_progress = time.time()
                    print(f"{time.time():.2f}: {self.name} has both resources!")

                    time.sleep(0.2)
                finally:
                    high.release()
            finally:
                low.release()


            time.sleep(0.05)


def main():
    resource_x = Resource("Resource X")
    resource_y = Resource("Resource Y")

    thread_a = Worker("Thread A", resource_x, resource_y)
    thread_b = Worker("Thread B", resource_y, resource_x)

    print("Starting workers...")
    thread_a.start()
    thread_b.start()


    time.sleep(3)
    thread_a.active = False
    thread_b.active = False

    thread_a.join()
    thread_b.join()
    print("Simulation completed without livelock or deadlock.")


if __name__ == "__main__":
    main()

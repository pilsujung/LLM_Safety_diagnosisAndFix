import threading
import time

class FairLock:
    """A simple FIFO (ticket) lock to prevent starvation."""
    def __init__(self):
        self._cond = threading.Condition()
        self._next_ticket = 0
        self._now_serving = 0

    def acquire(self):
        with self._cond:
            my_ticket = self._next_ticket
            self._next_ticket += 1
            while my_ticket != self._now_serving:
                self._cond.wait()

    def release(self):
        with self._cond:
            self._now_serving += 1
            self._cond.notify_all()


    def __enter__(self):
        self.acquire()
        return self
    def __exit__(self, exc_type, exc, tb):
        self.release()


class ResourceAllocator:
    def __init__(self):

        self.lock = FairLock()
        self.access_count = {
            'priority_thread': 0,
            'starved_thread': 0
        }
        self.resource_usage_log = []

    def priority_thread(self):
        """Behavior of the high-priority thread."""
        while self.access_count['priority_thread'] < 500:
            with self.lock:
                self.access_count['priority_thread'] += 1
                self.resource_usage_log.append(
                    f"Priority thread accessed at count {self.access_count['priority_thread']}"
                )
                print(f"Priority thread accessed resource {self.access_count['priority_thread']} times")
                time.sleep(0.01)
            time.sleep(0.005)

    def starved_thread(self):
        """Behavior of the thread that had difficulty accessing the resource."""
        while self.access_count['starved_thread'] < 500:

            with self.lock:
                self.access_count['starved_thread'] += 1
                self.resource_usage_log.append(
                    f"Starved thread accessed at count {self.access_count['starved_thread']}"
                )
                print(f"Starved thread accessed resource {self.access_count['starved_thread']} times")
            time.sleep(0.1)

    def simulate_starvation(self):
        """Simulate resource access between threads."""
        priority = threading.Thread(target=self.priority_thread, name="priority")
        starved = threading.Thread(target=self.starved_thread, name="starved")

        priority.start()
        starved.start()

        priority.join()
        starved.join()

        print("Final access counts:", self.access_count)
        self.log_resource_usage()

    def log_resource_usage(self):
        """Print resource usage log."""
        print("\nResource Usage Log:")
        for log_entry in self.resource_usage_log:
            print(log_entry)


if __name__ == "__main__":
    allocator = ResourceAllocator()
    allocator.simulate_starvation()

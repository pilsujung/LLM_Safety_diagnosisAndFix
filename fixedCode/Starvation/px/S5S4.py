import threading
import time

class ResourceAllocator:
    def __init__(self):
        self.lock = threading.Lock()
        self.access_count = {
            'priority_thread': 0,
            'starved_thread': 0
        }
        self.resource_usage_log = []
        self.fairness_lock = threading.Lock()
        self.starved_attempts = 0

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

    def starved_thread(self):
        """Behavior of the thread that has difficulty accessing the resource."""
        while self.access_count['starved_thread'] < 500:

            consecutive_fails = 0
            while consecutive_fails < 50:
                if self.lock.acquire(timeout=0.001):
                    try:
                        self.access_count['starved_thread'] += 1
                        self.resource_usage_log.append(
                            f"Starved thread accessed at count {self.access_count['starved_thread']}"
                        )
                        print(f"Starved thread accessed resource {self.access_count['starved_thread']} times")
                        consecutive_fails = 0
                        break
                    finally:
                        self.lock.release()
                else:
                    consecutive_fails += 1
            
            time.sleep(0.1)

    def simulate_starvation(self):
        """Simulate resource access between threads."""
        priority = threading.Thread(target=self.priority_thread, daemon=True)
        starved = threading.Thread(target=self.starved_thread, daemon=True)

        priority.start()
        starved.start()

        priority.join(timeout=30)
        starved.join(timeout=30)

        print("Final access counts:", self.access_count)

    def log_resource_usage(self):
        """Print resource usage log."""
        print("\nResource Usage Log:")
        for log_entry in self.resource_usage_log[-20:]:
            print(log_entry)

if __name__ == "__main__":
    allocator = ResourceAllocator()
    allocator.simulate_starvation()
    allocator.log_resource_usage()

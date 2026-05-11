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

        self.turn = 'starved_thread'
        self.turn_lock = threading.Lock()
        self.priority_consecutive = 0
        self.max_consecutive = 3

    def priority_thread(self):
        """Behavior of the high-priority thread with fairness."""
        while self.access_count['priority_thread'] < 500:

            with self.turn_lock:
                if self.priority_consecutive >= self.max_consecutive:

                    self.turn = 'starved_thread'
                    self.priority_consecutive = 0
                    time.sleep(0.02)
                    continue
            
            self.lock.acquire()
            try:
                self.access_count['priority_thread'] += 1
                self.resource_usage_log.append(
                    f"Priority thread accessed at count {self.access_count['priority_thread']}"
                )
                print(f"Priority thread accessed resource {self.access_count['priority_thread']} times")
                
                with self.turn_lock:
                    self.priority_consecutive += 1
                
                time.sleep(0.01)
            finally:
                self.lock.release()
            time.sleep(0.005)

    def starved_thread(self):
        """Behavior of the thread that previously had difficulty accessing the resource."""
        while self.access_count['starved_thread'] < 500:

            if self.lock.acquire(timeout=1.0):
                try:
                    self.access_count['starved_thread'] += 1
                    self.resource_usage_log.append(
                        f"Starved thread accessed at count {self.access_count['starved_thread']}"
                    )
                    print(f"Starved thread accessed resource {self.access_count['starved_thread']} times")
                    
                    with self.turn_lock:

                        self.priority_consecutive = 0
                        self.turn = 'priority_thread'
                finally:
                    self.lock.release()
                time.sleep(0.1)

    def simulate_starvation(self):
        """Simulate resource access between threads with fairness."""
        priority = threading.Thread(target=self.priority_thread)
        starved = threading.Thread(target=self.starved_thread)

        priority.start()
        starved.start()

        priority.join()
        starved.join()

        print("\nFinal access counts:", self.access_count)
        print(f"Priority thread: {self.access_count['priority_thread']} accesses")
        print(f"Starved thread: {self.access_count['starved_thread']} accesses")
        self.log_resource_usage()

    def log_resource_usage(self):
        """Print resource usage log summary."""
        print("\nResource Usage Summary:")
        print(f"Total log entries: {len(self.resource_usage_log)}")
        print("First 10 entries:")
        for log_entry in self.resource_usage_log[:10]:
            print(f"  {log_entry}")
        print("Last 10 entries:")
        for log_entry in self.resource_usage_log[-10:]:
            print(f"  {log_entry}")

if __name__ == "__main__":
    allocator = ResourceAllocator()
    allocator.simulate_starvation()
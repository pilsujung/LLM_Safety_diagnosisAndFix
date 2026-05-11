import threading
import time

class FairResourceAllocator:
    def __init__(self):
        self.lock = threading.RLock()
        self.access_count = {'priority_thread': 0, 'starved_thread': 0}
        self.resource_usage_log = []
        self.consecutive_priority = 0
        self.max_consecutive = 10
        self.condition = threading.Condition(self.lock)

    def priority_thread(self):
        while self.access_count['priority_thread'] < 500:
            with self.lock:
                self.consecutive_priority += 1
                if self.consecutive_priority >= self.max_consecutive:

                    self.condition.notify_all()
                    self.consecutive_priority = 0
                    self.lock.release()
                    time.sleep(0.05)
                    continue
                
                self.access_count['priority_thread'] += 1
                self.resource_usage_log.append(f"Priority#{self.access_count['priority_thread']}")
                print(f"Priority: {self.access_count['priority_thread']}")
                time.sleep(0.01)
            
            time.sleep(0.005)

    def starved_thread(self):
        consecutive_fails = 0
        while self.access_count['starved_thread'] < 500:
            acquired = self.lock.acquire(timeout=0.01)
            
            if acquired:
                try:
                    self.access_count['starved_thread'] += 1
                    self.resource_usage_log.append(f"Starved#{self.access_count['starved_thread']}")
                    print(f"Starved: {self.access_count['starved_thread']}")
                    consecutive_fails = 0
                finally:
                    self.lock.release()
                time.sleep(0.1)
            else:

                consecutive_fails += 1
                wait_time = min(0.001 * (2 ** min(consecutive_fails, 8)), 0.05)
                time.sleep(wait_time)

                with self.lock:
                    if self.consecutive_priority >= self.max_consecutive:
                        self.condition.wait(timeout=0.01)

    def simulate_fair_access(self):
        priority = threading.Thread(target=self.priority_thread, daemon=True)
        starved = threading.Thread(target=self.starved_thread, daemon=True)
        priority.start()
        starved.start()
        priority.join(timeout=30)
        starved.join(timeout=30)
        print(f"Final: {self.access_count}")

if __name__ == "__main__":
    allocator = FairResourceAllocator()
    allocator.simulate_fair_access()

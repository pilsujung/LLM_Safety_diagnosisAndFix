import threading
import time
from collections import deque

class ResourceAllocator:
    def __init__(self):
        self.lock = threading.Lock()
        self.access_count = {
            'priority_thread': 0,
            'starved_thread': 0
        }
        self.resource_usage_log = []

        self.queue = deque()
        self.queue_lock = threading.Lock()
        self.turn_events = {}

    def request_access(self, thread_name):
        """Request access to the resource with fair scheduling."""
        event = threading.Event()
        
        with self.queue_lock:
            self.queue.append(thread_name)
            self.turn_events[thread_name] = event
        

        while True:
            with self.queue_lock:
                if self.queue and self.queue[0] == thread_name:
                    break
            time.sleep(0.001)
        
        self.lock.acquire()

    def release_access(self, thread_name):
        """Release access to the resource."""
        self.lock.release()
        
        with self.queue_lock:
            if self.queue and self.queue[0] == thread_name:
                self.queue.popleft()
                if thread_name in self.turn_events:
                    del self.turn_events[thread_name]

    def priority_thread(self):
        """Behavior of the high-priority thread."""
        while self.access_count['priority_thread'] < 500:
            self.request_access('priority_thread')
            try:
                self.access_count['priority_thread'] += 1
                self.resource_usage_log.append(
                    f"Priority thread accessed at count {self.access_count['priority_thread']}"
                )
                print(f"Priority thread accessed resource {self.access_count['priority_thread']} times")
                time.sleep(0.01)
            finally:
                self.release_access('priority_thread')
            time.sleep(0.005)

    def starved_thread(self):
        """Behavior of the thread that previously had difficulty accessing the resource."""
        while self.access_count['starved_thread'] < 500:
            self.request_access('starved_thread')
            try:
                self.access_count['starved_thread'] += 1
                self.resource_usage_log.append(
                    f"Starved thread accessed at count {self.access_count['starved_thread']}"
                )
                print(f"Starved thread accessed resource {self.access_count['starved_thread']} times")
                time.sleep(0.01)
            finally:
                self.release_access('starved_thread')
            time.sleep(0.005)

    def simulate_starvation(self):
        """Simulate resource access between threads."""
        priority = threading.Thread(target=self.priority_thread)
        starved = threading.Thread(target=self.starved_thread)

        start_time = time.time()
        
        priority.start()
        starved.start()

        priority.join()
        starved.join()

        end_time = time.time()

        print("\n" + "="*50)
        print("Final access counts:", self.access_count)
        print(f"Execution time: {end_time - start_time:.2f} seconds")
        print("="*50)
        



    def log_resource_usage(self):
        """Print resource usage log."""
        print("\nResource Usage Log:")
        for log_entry in self.resource_usage_log:
            print(log_entry)

if __name__ == "__main__":
    allocator = ResourceAllocator()
    allocator.simulate_starvation()
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
        

        self.waiting_queue = deque()
        self.queue_lock = threading.Lock()
        self.turn_condition = threading.Condition(self.queue_lock)
        self.current_turn = None

    def request_access(self, thread_id):
        """Fair access request mechanism."""
        with self.queue_lock:

            self.waiting_queue.append(thread_id)
            

            while self.waiting_queue and self.waiting_queue[0] != thread_id:
                self.turn_condition.wait()
            

            self.current_turn = thread_id

    def release_access(self, thread_id):
        """Release access and notify next thread."""
        with self.queue_lock:

            if self.waiting_queue and self.waiting_queue[0] == thread_id:
                self.waiting_queue.popleft()
            
            self.current_turn = None

            self.turn_condition.notify_all()

    def priority_thread(self):
        """Behavior of the high-priority thread."""
        thread_id = 'priority_thread'
        
        while self.access_count[thread_id] < 500:

            self.request_access(thread_id)
            
            self.lock.acquire()
            try:
                self.access_count[thread_id] += 1
                self.resource_usage_log.append(
                    f"Priority thread accessed at count {self.access_count[thread_id]}"
                )
                print(f"Priority thread accessed resource {self.access_count[thread_id]} times")
                time.sleep(0.01)
            finally:
                self.lock.release()

                self.release_access(thread_id)
            
            time.sleep(0.005)

    def starved_thread(self):
        """Behavior of the thread that has difficulty accessing the resource."""
        thread_id = 'starved_thread'
        
        while self.access_count[thread_id] < 500:

            self.request_access(thread_id)
            
            self.lock.acquire()
            try:
                self.access_count[thread_id] += 1
                self.resource_usage_log.append(
                    f"Starved thread accessed at count {self.access_count[thread_id]}"
                )
                print(f"Starved thread accessed resource {self.access_count[thread_id]} times")
                time.sleep(0.01)
            finally:
                self.lock.release()
                self.release_access(thread_id)
            
            time.sleep(0.005)

    def simulate_starvation(self):
        """Simulate resource access between threads."""
        priority = threading.Thread(target=self.priority_thread)
        starved = threading.Thread(target=self.starved_thread)

        priority.start()
        starved.start()

        priority.join()
        starved.join()

        print("\nFinal access counts:", self.access_count)
        print(f"Priority thread: {self.access_count['priority_thread']}")
        print(f"Starved thread: {self.access_count['starved_thread']}")
        self.log_resource_usage()

    def log_resource_usage(self):
        """Print resource usage log summary."""
        print(f"\nTotal resource accesses: {len(self.resource_usage_log)}")
        print("First 10 accesses:")
        for log_entry in self.resource_usage_log[:10]:
            print(log_entry)
        print("Last 10 accesses:")
        for log_entry in self.resource_usage_log[-10:]:
            print(log_entry)

if __name__ == "__main__":
    allocator = ResourceAllocator()
    allocator.simulate_starvation()
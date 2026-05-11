import threading 
import time 
from collections import deque

class ResourceAllocator: 
    def __init__(self): 
        self.lock = threading.Lock() 
        self.queue_lock = threading.Lock()
        self.waiting_queue = deque()
        self.access_count = { 
            'priority_thread': 0, 
            'starved_thread': 0 
        } 
        self.resource_usage_log = []
 
    def acquire_fair(self, thread_id):
        """Fair lock acquisition using a queue."""
        event = threading.Event()
        
        with self.queue_lock:
            self.waiting_queue.append((thread_id, event))
        

        while True:
            with self.queue_lock:
                if self.waiting_queue and self.waiting_queue[0][0] == thread_id:

                    if self.lock.acquire(blocking=False):
                        self.waiting_queue.popleft()
                        return True
            time.sleep(0.001)
    
    def release_fair(self):
        """Release the lock."""
        self.lock.release()

    def priority_thread(self): 
        """Behavior of the high-priority thread.""" 
        while self.access_count['priority_thread'] < 500: 
            self.acquire_fair('priority_thread')
            try: 
                self.access_count['priority_thread'] += 1 
                self.resource_usage_log.append( 
                    f"Priority thread accessed at count {self.access_count['priority_thread']}" 
                ) 
                print(f"Priority thread accessed resource {self.access_count['priority_thread']} times") 
                time.sleep(0.01)
            finally: 
                self.release_fair()
            time.sleep(0.005)
 
    def starved_thread(self): 
        """Behavior of the thread that has difficulty accessing the resource.""" 
        while self.access_count['starved_thread'] < 500: 
            self.acquire_fair('starved_thread')
            try: 
                self.access_count['starved_thread'] += 1 
                self.resource_usage_log.append( 
                    f"Starved thread accessed at count {self.access_count['starved_thread']}" 
                ) 
                print(f"Starved thread accessed resource {self.access_count['starved_thread']} times") 
            finally: 
                self.release_fair()
            time.sleep(0.1)
 
    def simulate_starvation(self): 
        """Simulate resource access between threads.""" 
        priority = threading.Thread(target=self.priority_thread) 
        starved = threading.Thread(target=self.starved_thread) 
 
        priority.start() 
        starved.start() 
 
        priority.join() 
        starved.join() 
 
        print("\nFinal access counts:", self.access_count) 
        self.log_resource_usage() 
 
    def log_resource_usage(self): 
        """Print resource usage log.""" 
        print("\nResource Usage Log (last 20 entries):") 
        for log_entry in self.resource_usage_log[-20:]: 
            print(log_entry) 
 
if __name__ == "__main__": 
    allocator = ResourceAllocator() 
    allocator.simulate_starvation()
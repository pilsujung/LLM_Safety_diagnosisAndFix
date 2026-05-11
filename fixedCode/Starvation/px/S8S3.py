import threading
import time
import random
from collections import deque


class FairResource:
    def __init__(self, max_usage_time=0.2):
        self._lock = threading.Lock()
        self._resource_available = threading.Condition(self._lock)
        self._owner = None
        self._usage_start = None
        self.max_usage_time = max_usage_time
        


        self._wait_queue = deque()
        self._stats_lock = threading.Lock()
        self.thread_stats = {}
    
    def initialize_stats(self, thread_id):
        with self._stats_lock:
            self.thread_stats[thread_id] = {
                'access_count': 0,
                'total_wait_time': 0,
                'total_usage_time': 0,
                'last_access_time': None,
                'is_lightweight': False
            }
    
    def update_stats(self, thread_id, wait_time, usage_time):
        with self._stats_lock:
            stats = self.thread_stats[thread_id]
            stats['access_count'] += 1
            stats['total_wait_time'] += wait_time
            stats['total_usage_time'] += usage_time
            stats['last_access_time'] = time.time()
    
    def get_stats(self):
        with self._stats_lock:
            return {tid: stats.copy() for tid, stats in self.thread_stats.items()}
    
    def acquire(self, thread_id, is_lightweight=False):
        self.initialize_stats(thread_id)
        
        with self._lock:
            wait_start = time.time()
            

            priority = 0 if is_lightweight else 1
            

            self._wait_queue.append((priority, thread_id, wait_start))
            
            while True:

                while self._wait_queue and self._wait_queue[0][1] == self._owner:
                    self._wait_queue.popleft()
                

                if self._wait_queue and self._wait_queue[0][1] == thread_id:

                    self._wait_queue.popleft()
                    self._owner = thread_id
                    self._usage_start = time.time()
                    break
                

                self._resource_available.wait()
        
        wait_end = time.time()
        wait_duration = wait_end - wait_start
        
        print(f"[{ 'LIGHT' if is_lightweight else 'GREEDY' }] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")
        
        return wait_duration
    
    def release(self, thread_id):
        actual_usage_time = 0
        
        with self._lock:
            if self._owner == thread_id:
                if self._usage_start:
                    actual_usage_time = time.time() - self._usage_start
                

                if actual_usage_time > self.max_usage_time:
                    print(f"[WARNING] Thread {thread_id} held resource {actual_usage_time:.3f}s > {self.max_usage_time}s")
                
                self._owner = None
                self._usage_start = None
                self._resource_available.notify_all()
        
        print(f"[{ 'LIGHT' if self.thread_stats[thread_id]['is_lightweight'] else 'GREEDY' }] Thread {thread_id} released resource after {actual_usage_time:.3f}s")
        return actual_usage_time

def print_stats(fair_resource):
    stats = fair_resource.get_stats()
    print("\n" + "="*60)
    print("THREAD STATISTICS (FAIR SCHEDULING)")
    print("="*60)
    for thread_id, stat in stats.items():
        avg_wait = stat['total_wait_time'] / max(stat['access_count'], 1)
        print(f"Thread {thread_id} ({'LIGHT' if stat['is_lightweight'] else 'GREEDY'}):")
        print(f"  - Access count: {stat['access_count']}")
        print(f"  - Average wait time: {avg_wait:.3f}s")
        print(f"  - Total usage time: {stat['total_usage_time']:.3f}s")
        if stat['last_access_time']:
            time_since_last = time.time() - stat['last_access_time']
            print(f"  - Time since last access: {time_since_last:.3f}s")
    print("="*60 + "\n")

def greedy_resource_user(fair_resource, thread_id, base_hold_time, variation_factor=0.1):
    iteration_count = 0
    while iteration_count < 20:
        wait_duration = fair_resource.acquire(thread_id, is_lightweight=False)
        fair_resource.thread_stats[thread_id]['is_lightweight'] = False
        
        actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
        print(f"[GREEDY] Thread {thread_id} will hold resource for {actual_hold_time:.3f}s")
        
        time.sleep(actual_hold_time)
        usage_time = fair_resource.release(thread_id)
        
        fair_resource.update_stats(thread_id, wait_duration, usage_time)
        time.sleep(0.01)
        iteration_count += 1

def lightweight_resource_user(fair_resource, thread_id, base_hold_time, variation_factor=0.05):
    iteration_count = 0
    while iteration_count < 50:
        wait_duration = fair_resource.acquire(thread_id, is_lightweight=True)
        fair_resource.thread_stats[thread_id]['is_lightweight'] = True
        
        actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
        time.sleep(actual_hold_time)
        usage_time = fair_resource.release(thread_id)
        
        fair_resource.update_stats(thread_id, wait_duration, usage_time)
        time.sleep(0.02)
        iteration_count += 1

def monitor_thread(fair_resource):
    time.sleep(2)
    for i in range(5):
        time.sleep(3)
        print_stats(fair_resource)

def main():
    print("Fair Thread Scheduling - No More Starvation!")
    print("="*60)
    print("Uses priority queue: lightweight threads prioritized over greedy threads")
    print("="*60 + "\n")
    
    fair_resource = FairResource(max_usage_time=0.25)
    
    threads = []
    greedy_thread_1 = threading.Thread(
        target=greedy_resource_user, 
        args=(fair_resource, 1, 1.0, 0.2),
        name="GreedyThread-1"
    )
    greedy_thread_2 = threading.Thread(
        target=greedy_resource_user, 
        args=(fair_resource, 2, 0.8, 0.3),
        name="GreedyThread-2"
    )
    light_thread_1 = threading.Thread(
        target=lightweight_resource_user, 
        args=(fair_resource, 3, 0.1, 0.02),
        name="LightThread-1"
    )
    light_thread_2 = threading.Thread(
        target=lightweight_resource_user, 
        args=(fair_resource, 4, 0.05, 0.01),
        name="LightThread-2"
    )
    monitor = threading.Thread(target=monitor_thread, args=(fair_resource,), name="Monitor")
    
    threads.extend([greedy_thread_1, greedy_thread_2, light_thread_1, light_thread_2, monitor])
    
    print("Starting all threads...\n")
    for thread in threads:
        thread.start()
        time.sleep(0.1)
    
    for thread in threads:
        thread.join()
    
    print("\nFINAL RESULTS (FAIR SCHEDULING):")
    print_stats(fair_resource)
    print("Success! Lightweight threads now get fair access with minimal wait times.")

if __name__ == "__main__":
    main()

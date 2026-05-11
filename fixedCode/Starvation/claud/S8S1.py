import threading
import time
import random
from collections import deque


class FairLock:
    """A lock that ensures fair access by queuing waiting threads"""
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = deque()
        self._queue_lock = threading.Lock()
    
    def acquire(self):

        my_event = threading.Event()
        

        with self._queue_lock:
            self._queue.append(my_event)

            if len(self._queue) == 1:
                my_event.set()
        

        my_event.wait()
        

        self._lock.acquire()
    
    def release(self):

        self._lock.release()
        

        with self._queue_lock:
            if self._queue:
                self._queue.popleft()

            if self._queue:
                self._queue[0].set()
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


shared_resource_lock = FairLock()


thread_stats = {}
stats_lock = threading.Lock()

def initialize_stats(thread_id):
    """Initialize statistics for a thread"""
    with stats_lock:
        thread_stats[thread_id] = {
            'access_count': 0,
            'total_wait_time': 0,
            'total_usage_time': 0,
            'last_access_time': None
        }

def update_stats(thread_id, wait_time, usage_time):
    """Update statistics for a thread"""
    with stats_lock:
        stats = thread_stats[thread_id]
        stats['access_count'] += 1
        stats['total_wait_time'] += wait_time
        stats['total_usage_time'] += usage_time
        stats['last_access_time'] = time.time()

def print_stats():
    """Print current statistics for all threads"""
    with stats_lock:
        print("\n" + "="*60)
        print("THREAD STATISTICS")
        print("="*60)
        for thread_id, stats in thread_stats.items():
            avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
            print(f"Thread {thread_id}:")
            print(f"  - Access count: {stats['access_count']}")
            print(f"  - Average wait time: {avg_wait:.3f}s")
            print(f"  - Total usage time: {stats['total_usage_time']:.3f}s")
            if stats['last_access_time']:
                time_since_last = time.time() - stats['last_access_time']
                print(f"  - Time since last access: {time_since_last:.3f}s")
        print("="*60 + "\n")

def greedy_resource_user(thread_id, base_hold_time, variation_factor=0.1):
    """
    A greedy thread that holds the resource for a long time
    
    Args:
        thread_id: Unique identifier for the thread
        base_hold_time: Base time to hold the resource
        variation_factor: Random variation in hold time (0.0 to 1.0)
    """
    initialize_stats(thread_id)
    iteration_count = 0
    
    while iteration_count < 20:
        wait_start_time = time.time()
        
        with shared_resource_lock:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time
            
            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
            
            print(f"[GREEDY] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")
            print(f"[GREEDY] Thread {thread_id} will hold resource for {actual_hold_time:.3f}s")
            
            usage_start_time = time.time()
            time.sleep(actual_hold_time)
            usage_end_time = time.time()
            actual_usage_time = usage_end_time - usage_start_time
            
            print(f"[GREEDY] Thread {thread_id} releasing resource after {actual_usage_time:.3f}s")
            
            update_stats(thread_id, wait_duration, actual_usage_time)
            

        time.sleep(0.1)
        iteration_count += 1

def lightweight_resource_user(thread_id, base_hold_time, variation_factor=0.05):
    """
    A lightweight thread that needs quick access to the resource
    
    Args:
        thread_id: Unique identifier for the thread
        base_hold_time: Base time to hold the resource (should be small)
        variation_factor: Random variation in hold time
    """
    initialize_stats(thread_id)
    iteration_count = 0
    
    while iteration_count < 50:
        wait_start_time = time.time()
        
        with shared_resource_lock:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time
            
            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
            
            print(f"[LIGHT] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")
            
            usage_start_time = time.time()
            time.sleep(actual_hold_time)
            usage_end_time = time.time()
            actual_usage_time = usage_end_time - usage_start_time
            
            print(f"[LIGHT] Thread {thread_id} released resource after {actual_usage_time:.3f}s")
            
            update_stats(thread_id, wait_duration, actual_usage_time)
            
        time.sleep(0.02)
        iteration_count += 1

def monitor_thread():
    """Monitor thread that periodically prints statistics"""
    time.sleep(2)
    
    for i in range(5):
        time.sleep(3)
        print_stats()

def main():
    """Main function to demonstrate fair thread scheduling"""
    print("Starting Fair Thread Scheduling Demonstration")
    print("="*60)
    print("This program uses a fair lock to prevent thread starvation")
    print("Greedy threads hold the resource for ~1.0s")
    print("Lightweight threads need the resource for only ~0.1s")
    print("All threads get fair access via queue-based scheduling")
    print("="*60 + "\n")
    
    threads = []
    
    greedy_thread_1 = threading.Thread(
        target=greedy_resource_user, 
        args=(1, 1.0, 0.2),
        name="GreedyThread-1"
    )
    
    greedy_thread_2 = threading.Thread(
        target=greedy_resource_user, 
        args=(2, 0.8, 0.3),
        name="GreedyThread-2"
    )
    
    light_thread_1 = threading.Thread(
        target=lightweight_resource_user, 
        args=(3, 0.1, 0.02),
        name="LightThread-1"
    )
    
    light_thread_2 = threading.Thread(
        target=lightweight_resource_user, 
        args=(4, 0.05, 0.01),
        name="LightThread-2"
    )
    
    monitor = threading.Thread(target=monitor_thread, name="Monitor")
    
    threads.extend([greedy_thread_1, greedy_thread_2, light_thread_1, light_thread_2, monitor])
    
    print("Starting all threads...\n")
    for thread in threads:
        thread.start()
        time.sleep(0.1)
    
    for thread in threads:
        thread.join()
    
    print("\nFINAL RESULTS:")
    print_stats()
    
    print("Demonstration complete!")
    print("\nObservation: With fair scheduling, all threads get reasonable access")
    print("to the shared resource, preventing starvation.")

if __name__ == "__main__":
    main()
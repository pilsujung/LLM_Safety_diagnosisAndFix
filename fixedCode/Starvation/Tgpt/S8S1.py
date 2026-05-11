import threading
import time
import random



class FairLock:
    """
    A simple fair (FIFO) lock using a ticket mechanism.
    Threads acquire a ticket and wait until their ticket is served.
    """
    def __init__(self):
        self._state_lock = threading.Lock()
        self._cond = threading.Condition(self._state_lock)
        self._next_ticket = 0
        self._serving_ticket = 0

    def acquire(self):
        with self._state_lock:
            my_ticket = self._next_ticket
            self._next_ticket += 1


            while my_ticket != self._serving_ticket:
                self._cond.wait()

    def release(self):
        with self._state_lock:
            self._serving_ticket += 1
            self._cond.notify_all()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()



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
            
        time.sleep(0.01)
        iteration_count += 1

def lightweight_resource_user(thread_id, base_hold_time, variation_factor=0.05):
    """
    A lightweight thread that needs quick access to the resource
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
    
    for _ in range(5):
        time.sleep(3)
        print_stats()

def main():
    """Main function to demonstrate starvation-free behavior"""
    print("Starting Thread Starvation Demonstration (fair lock version)")
    print("="*60)
    print("Greedy threads still hold the resource longer,")
    print("but a fair lock ensures lightweight threads can't be starved.")
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
    for t in threads:
        t.start()
        time.sleep(0.1)
    
    for t in threads:
        t.join()
    
    print("\nFINAL RESULTS:")
    print_stats()
    
    print("Demonstration complete!")
    print("\nObservation: Lightweight threads are no longer starved")
    print("because the fair lock forces a roughly FIFO order.")

if __name__ == "__main__":
    main()

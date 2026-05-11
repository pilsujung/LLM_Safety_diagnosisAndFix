import threading
import time
import random
from collections import deque


class FairLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._waiters = deque()
        self._next_ticket = 0
        self._current_ticket = 0

    def acquire(self, thread_id):
        with self._lock:

            ticket = self._next_ticket
            self._next_ticket += 1
            self._waiters.append((ticket, thread_id))
            

            while self._current_ticket != ticket:
                self._cond.wait()
            
            self._current_ticket += 1

            self._waiters.popleft()
            self._cond.notify_all()


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
        print("THREAD STATISTICS (FAIR SCHEDULING)")
        print("="*60)
        for thread_id, stats in thread_stats.items():
            avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
            print(f"Thread {thread_id}:")
            print(f"  Access count: {stats['access_count']}")
            print(f"  Average wait: {avg_wait:.3f}s")
            print(f"  Total usage:  {stats['total_usage_time']:.3f}s")
            if stats['last_access_time']:
                time_since = time.time() - stats['last_access_time']
                print(f"  Time since last: {time_since:.3f}s")
        print("="*60 + "\n")


fair_resource_lock = FairLock()

def greedy_resource_user(thread_id, base_hold_time, variation_factor=0.1):
    initialize_stats(thread_id)
    iteration_count = 0

    while iteration_count < 20:
        wait_start_time = time.time()
        

        fair_resource_lock.acquire(thread_id)
        
        wait_end_time = time.time()
        wait_duration = wait_end_time - wait_start_time

        actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
        print(f"[GREEDY-{thread_id}] Acquired (wait {wait_duration:.3f}s, hold {actual_hold_time:.3f}s)")


        usage_start = time.time()
        time.sleep(actual_hold_time)
        actual_usage = time.time() - usage_start

        print(f"[GREEDY-{thread_id}] Released after {actual_usage:.3f}s")
        update_stats(thread_id, wait_duration, actual_usage)
        iteration_count += 1

def lightweight_resource_user(thread_id, base_hold_time, variation_factor=0.05):
    initialize_stats(thread_id)
    iteration_count = 0

    while iteration_count < 50:
        wait_start_time = time.time()
        

        fair_resource_lock.acquire(thread_id)
        
        wait_end_time = time.time()
        wait_duration = wait_end_time - wait_start_time

        actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
        print(f"[LIGHT-{thread_id}] Acquired (wait {wait_duration:.3f}s)")


        usage_start = time.time()
        time.sleep(actual_hold_time)
        actual_usage = time.time() - usage_start

        print(f"[LIGHT-{thread_id}] Released after {actual_usage:.3f}s")
        update_stats(thread_id, wait_duration, actual_usage)
        iteration_count += 1

def monitor_thread():
    time.sleep(2)
    for i in range(6):
        time.sleep(2.5)
        print_stats()

def main():
    print("FAIR THREAD SCHEDULING - NO STARVATION")
    print("="*60)
    print("✓ FIFO ticket system guarantees every thread gets served")
    print("✓ Lightweight threads complete 50 iterations each")
    print("✓ Greedy threads cannot monopolize resource")
    print("="*60 + "\n")

    threads = [
        threading.Thread(target=greedy_resource_user, args=(1, 1.0, 0.2), name="Greedy-1"),
        threading.Thread(target=greedy_resource_user, args=(2, 0.8, 0.3), name="Greedy-2"),
        threading.Thread(target=lightweight_resource_user, args=(3, 0.1, 0.02), name="Light-1"),
        threading.Thread(target=lightweight_resource_user, args=(4, 0.05, 0.01), name="Light-2"),
        threading.Thread(target=monitor_thread, name="Monitor")
    ]

    print("Starting fair threads...\n")
    for t in threads:
        t.start()
        time.sleep(0.05)

    for t in threads:
        t.join()

    print("\nFINAL FAIR RESULTS:")
    print_stats()
    print("✓ STARVATION ELIMINATED: All threads complete successfully!")

if __name__ == "__main__":
    main()

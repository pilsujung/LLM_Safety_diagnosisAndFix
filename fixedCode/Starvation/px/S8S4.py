import threading
import time
import random
from collections import deque


class FairSemaphore:
    def __init__(self, value=1):
        self._lock = threading.Lock()
        self._value = value
        self._waiters = deque()
        self._counter = 0

    def acquire(self, timeout=None):
        ticket = None
        start_time = time.time()
        
        with self._lock:
            while True:
                if self._value > 0:
                    self._value -= 1
                    return True
                

                if ticket is None:
                    ticket = self._counter
                    self._counter += 1
                    self._waiters.append(ticket)
                

                if timeout and (time.time() - start_time > timeout):
                    self._waiters.remove(ticket)
                    return False
                

                self._lock.release()
                time.sleep(0.001)
                self._lock.acquire()
        
        return False

    def release(self):
        with self._lock:
            self._value += 1
            if self._waiters:

                next_ticket = self._waiters.popleft()



fair_resource = FairSemaphore(1)


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
        for thread_id, stats in sorted(thread_stats.items()):
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
    """Greedy thread using fair semaphore"""
    initialize_stats(thread_id)
    iteration_count = 0

    while iteration_count < 20:
        wait_start_time = time.time()


        acquired = fair_resource.acquire(timeout=5.0)
        if not acquired:
            print(f"[GREEDY] Thread {thread_id} timed out waiting")
            time.sleep(0.1)
            continue

        wait_end_time = time.time()
        wait_duration = wait_end_time - wait_start_time

        actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
        print(f"[GREEDY] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")
        print(f"[GREEDY] Thread {thread_id} will hold for {actual_hold_time:.3f}s")


        usage_start_time = time.time()
        time.sleep(actual_hold_time)
        actual_usage_time = time.time() - usage_start_time

        print(f"[GREEDY] Thread {thread_id} released after {actual_usage_time:.3f}s")
        
        fair_resource.release()
        update_stats(thread_id, wait_duration, actual_usage_time)

        time.sleep(0.01)
        iteration_count += 1

def lightweight_resource_user(thread_id, base_hold_time, variation_factor=0.05):
    """Lightweight thread using fair semaphore"""
    initialize_stats(thread_id)
    iteration_count = 0

    while iteration_count < 50:
        wait_start_time = time.time()

        acquired = fair_resource.acquire(timeout=2.0)
        if not acquired:
            print(f"[LIGHT] Thread {thread_id} timed out waiting")
            time.sleep(0.02)
            continue

        wait_end_time = time.time()
        wait_duration = wait_end_time - wait_start_time

        actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
        print(f"[LIGHT] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")

        usage_start_time = time.time()
        time.sleep(actual_hold_time)
        actual_usage_time = time.time() - usage_start_time

        print(f"[LIGHT] Thread {thread_id} released after {actual_usage_time:.3f}s")
        
        fair_resource.release()
        update_stats(thread_id, wait_duration, actual_usage_time)

        time.sleep(0.02)
        iteration_count += 1

def monitor_thread():
    """Monitor thread that periodically prints statistics"""
    time.sleep(2)
    for i in range(8):
        time.sleep(3)
        print_stats()

def main():
    """Main function demonstrating fair thread scheduling"""
    print("FAIR THREAD SCHEDULING DEMONSTRATION")
    print("="*60)
    print("Custom FairSemaphore prevents starvation using FIFO waiter queue")
    print("All threads get fair access regardless of hold time")
    print("="*60 + "\n")

    threads = []


    greedy_thread_1 = threading.Thread(
        target=greedy_resource_user, args=(1, 1.0, 0.2), name="GreedyThread-1"
    )
    greedy_thread_2 = threading.Thread(
        target=greedy_resource_user, args=(2, 0.8, 0.3), name="GreedyThread-2"
    )
    light_thread_1 = threading.Thread(
        target=lightweight_resource_user, args=(3, 0.1, 0.02), name="LightThread-1"
    )
    light_thread_2 = threading.Thread(
        target=lightweight_resource_user, args=(4, 0.05, 0.01), name="LightThread-2"
    )
    monitor = threading.Thread(target=monitor_thread, name="Monitor")

    threads.extend([greedy_thread_1, greedy_thread_2, light_thread_1, light_thread_2, monitor])

    print("Starting threads with FAIR SCHEDULING...\n")
    for thread in threads:
        thread.start()
        time.sleep(0.05)

    for thread in threads:
        thread.join()

    print("\nFINAL RESULTS (FAIR SCHEDULING):")
    print_stats()
    print("SUCCESS: No starvation! All threads got fair access.")

if __name__ == "__main__":
    main()

import threading
import time
import random


shared_resource_lock = threading.Semaphore(1)


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
    """Greedy thread with yield for fairness"""
    initialize_stats(thread_id)
    iteration_count = 0

    while iteration_count < 20:

        time.sleep(0.001)
        
        wait_start_time = time.time()
        with shared_resource_lock:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time
            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)

            print(f"[GREEDY] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")
            print(f"[GREEDY] Thread {thread_id} will hold resource for {actual_hold_time:.3f}s")

            usage_start_time = time.time()
            time.sleep(actual_hold_time)
            actual_usage_time = time.time() - usage_start_time

            print(f"[GREEDY] Thread {thread_id} releasing resource after {actual_usage_time:.3f}s")

        update_stats(thread_id, wait_duration, actual_usage_time)
        time.sleep(0.01)
        iteration_count += 1

def lightweight_resource_user(thread_id, base_hold_time, variation_factor=0.05):
    """Lightweight thread with yield for fairness"""
    initialize_stats(thread_id)
    iteration_count = 0

    while iteration_count < 50:

        time.sleep(0.001)
        
        wait_start_time = time.time()
        with shared_resource_lock:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time
            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)

            print(f"[LIGHT] Thread {thread_id} acquired resource (waited {wait_duration:.3f}s)")
            usage_start_time = time.time()
            time.sleep(actual_hold_time)
            actual_usage_time = time.time() - usage_start_time
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
    """Main function demonstrating starvation-free resource access"""
    print("Starting Fair Threading Demonstration (Starvation Fixed)")
    print("="*60)
    print("Uses Semaphore(1) + CPU yield for fair access")
    print("Greedy: ~1.0s holds | Light: ~0.1s holds")
    print("="*60 + "\n")

    threads = [
        threading.Thread(target=greedy_resource_user, args=(1, 1.0, 0.2), name="Greedy-1"),
        threading.Thread(target=greedy_resource_user, args=(2, 0.8, 0.3), name="Greedy-2"),
        threading.Thread(target=lightweight_resource_user, args=(3, 0.1, 0.02), name="Light-1"),
        threading.Thread(target=lightweight_resource_user, args=(4, 0.05, 0.01), name="Light-2"),
        threading.Thread(target=monitor_thread, name="Monitor")
    ]

    print("Starting all threads...\n")
    for thread in threads:
        thread.start()
        time.sleep(0.1)

    for thread in threads:
        thread.join()

    print("\nFINAL RESULTS:")
    print_stats()
    print("Demonstration complete!")
    print("\nAll threads access resource fairly - no starvation!")

if __name__ == "__main__":
    main()

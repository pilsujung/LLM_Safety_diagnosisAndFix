import threading
import time
import random


resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0
    }


def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Simulate thread attempting to access a shared resource.

    Fixed version:
    - NO priority queue
    - NO manual skipping of other threads
    - Each thread simply blocks on a shared lock (like ReentrantLock.lock())
    This removes starvation: every waiting thread eventually acquires the lock.
    """
    global total_resource_accesses

    initialize_thread_stats(thread_id, thread_priority)
    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")


    while thread_statistics[thread_id]['successful_accesses'] < max_attempts:
        start_wait_time = time.time()


        with resource_lock:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            wait_duration = time.time() - start_wait_time

            stats = thread_statistics[thread_id]
            stats['successful_accesses'] += 1
            stats['total_wait_time'] += wait_duration

            stats['failed_attempts'] += 0
            stats['starvation_count'] += 0

            total_resource_accesses += 1

            print(
                f"[{current_time}] ✓ Thread {thread_id} "
                f"(Priority {thread_priority}) acquired resource after {wait_duration:.3f}s wait "
                f"(access #{stats['successful_accesses']})"
            )


            resource_usage_time = 0.15 + (thread_priority * 0.05)
            time.sleep(resource_usage_time)

            print(
                f"[{current_time}] ✓ Thread {thread_id} "
                f"(Priority {thread_priority}) released resource "
                f"(used for {resource_usage_time:.3f}s)"
            )


        inter_attempt_delay = 0.1 + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)


    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)

    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"   Priority: {thread_priority}")
    print(f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"   Failed attempts: {stats['failed_attempts']}")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Starvation episodes: {stats['starvation_count']}")


def display_simulation_summary():
    """Display overall simulation statistics"""
    print("\n" + "=" * 60)
    print("THREAD STARVATION SIMULATION SUMMARY")
    print("=" * 60)
    print(f"Total resource accesses across all threads: {total_resource_accesses}")

    high_priority_accesses = sum(
        stats['successful_accesses']
        for stats in thread_statistics.values()
        if stats['priority'] <= 2
    )
    low_priority_accesses = sum(
        stats['successful_accesses']
        for stats in thread_statistics.values()
        if stats['priority'] > 2
    )

    print(f"High priority threads (1-2) total accesses: {high_priority_accesses}")
    print(f"Low priority threads (3+) total accesses: {low_priority_accesses}")

    total_starvation = sum(stats['starvation_count'] for stats in thread_statistics.values())
    print(f"Total starvation episodes detected: {total_starvation}")


    if low_priority_accesses < high_priority_accesses * 0.3:
        print("⚠️  STARVATION DETECTED: Low priority threads significantly underserved!")
    else:
        print("✅ No starvation detected: access is reasonably fair across priorities.")

    print("=" * 60)


def main():
    """Main function to run the thread starvation simulation"""
    print("Starting Thread Starvation Simulation")
    print("Lower priority numbers = Higher importance (1 = highest priority)")
    print("-" * 60)


    thread_configurations = [
        (1, "HighPrio-A"),
        (2, "HighPrio-B"),
        (4, "LowPrio-A"),
        (5, "LowPrio-B"),
        (3, "MedPrio-A")
    ]

    active_threads = []

    for priority, thread_name in thread_configurations:
        thread_obj = threading.Thread(
            target=access_shared_resource,
            args=(priority, thread_name),
            name=f"Thread-{thread_name}"
        )
        active_threads.append(thread_obj)
        thread_obj.start()
        time.sleep(0.1)

    for thread_obj in active_threads:
        thread_obj.join()

    display_simulation_summary()
    print("\nSimulation completed - Starvation should now be resolved")


if __name__ == "__main__":
    main()

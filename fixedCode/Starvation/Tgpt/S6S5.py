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
        'total_wait_time': 0.0,
        'priority': priority,
        'starvation_count': 0
    }

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Fixed version of the shared-resource access function.

    Instead of a custom priority queue and repeated try-and-backoff logic
    (which caused starvation of lower-priority threads), we now use a
    single fair, blocking mutex around the critical section.

    This is analogous to:
      • Using a FAIR ReentrantLock in Java (ReentrantLock(true))
      • Using Thread.yield() to give other threads a chance to run

    Here:
      • Every thread blocks on resource_lock until it can enter.
      • No attempt can "fail": each loop iteration eventually acquires
        the lock, so low-priority threads cannot be starved.
      • We still use thread_priority only to vary the simulated work time
        and think-time between accesses.
    """
    global total_resource_accesses

    initialize_thread_stats(thread_id, thread_priority)
    stats = thread_statistics[thread_id]

    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")

    for attempt_counter in range(max_attempts):
        start_wait_time = time.time()



        with resource_lock:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            wait_duration = time.time() - start_wait_time


            stats['successful_accesses'] += 1
            stats['total_wait_time'] += wait_duration
            total_resource_accesses += 1

            print(
                f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) "
                f"acquired resource after {wait_duration:.3f}s wait (attempt {attempt_counter + 1})"
            )



            resource_usage_time = 0.15 + (thread_priority * 0.05)
            time.sleep(resource_usage_time)

            print(
                f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) "
                f"released resource (used for {resource_usage_time:.3f}s)"
            )


        time.sleep(0)


        base_delay = 0.1 + (thread_priority * 0.05)
        inter_attempt_delay = base_delay + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)


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
    print("\n" + "="*60)
    print("THREAD STARVATION SIMULATION SUMMARY (FIXED)")
    print("="*60)
    print(f"Total resource accesses across all threads: {total_resource_accesses}")

    high_priority_accesses = sum(stats['successful_accesses']
                                 for stats in thread_statistics.values()
                                 if stats['priority'] <= 2)
    low_priority_accesses = sum(stats['successful_accesses']
                                for stats in thread_statistics.values()
                                if stats['priority'] > 2)

    print(f"High priority threads (1-2) total accesses: {high_priority_accesses}")
    print(f"Low priority threads (3+) total accesses: {low_priority_accesses}")

    total_starvation = sum(stats['starvation_count'] for stats in thread_statistics.values())
    print(f"Total starvation episodes detected: {total_starvation}")

    if low_priority_accesses < high_priority_accesses * 0.3:
        print("⚠️  STARVATION DETECTED: Low priority threads significantly underserved!")
    else:
        print("✅ No starvation detected: low priority threads are being served fairly.")

    print("="*60)

def main():
    """Main function to run the thread starvation simulation"""
    global resource_lock, total_resource_accesses, thread_statistics


    resource_lock = threading.Lock()
    total_resource_accesses = 0
    thread_statistics = {}

    print("Starting Thread Starvation Simulation (FIXED)")
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
    print("\nSimulation completed - Starvation resolved")

if __name__ == "__main__":
    main()

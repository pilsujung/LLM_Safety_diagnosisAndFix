import threading
import time
import random


resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


queue_condition = threading.Condition()
next_ticket = 0
now_serving = 0


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
    Simulate fair access to a shared resource.

    FIXED VERSION:
    -------------
    The original version used a PriorityQueue and a busy loop that kept
    re-enqueuing other threads, which allowed high-priority threads to
    dominate and caused starvation for low-priority threads.

    Here we use a *ticket lock* scheme implemented with a Condition:
      - Each access attempt gets a monotonically increasing ticket number.
      - Threads wait until their ticket is the one being served.
      - This guarantees FIFO ordering and prevents starvation completely.

    We still keep `thread_priority` for stats and to slightly adjust how long
    each thread holds the resource, but priority no longer affects *who*
    gets the resource next (fairness > priority).
    """
    global total_resource_accesses, next_ticket, now_serving

    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0

    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")

    while attempt_counter < max_attempts:
        start_wait_time = time.time()


        with queue_condition:
            my_ticket = next_ticket
            next_ticket += 1


            while my_ticket != now_serving:
                queue_condition.wait()


        resource_acquired = False
        with resource_lock:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            wait_duration = time.time() - start_wait_time


            stats = thread_statistics[thread_id]
            stats['successful_accesses'] += 1
            stats['total_wait_time'] += wait_duration
            total_resource_accesses += 1
            consecutive_failures = 0
            resource_acquired = True

            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) "
                  f"acquired resource after {wait_duration:.3f}s wait")




            base_usage = 0.25
            per_level_bonus = 0.03

            usage_time = max(0.05, base_usage + (thread_priority - 3) * per_level_bonus)
            time.sleep(usage_time)

            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) "
                  f"released resource (used for {usage_time:.3f}s)")


        with queue_condition:
            now_serving += 1
            queue_condition.notify_all()



        if not resource_acquired:
            consecutive_failures += 1
            stats = thread_statistics[thread_id]
            stats['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())

            if consecutive_failures >= 3:
                stats['starvation_count'] += 1
                print(f"[{current_time}] ⚠️  Thread {thread_id} (Priority {thread_priority}) "
                      f"experiencing STARVATION - {consecutive_failures} consecutive failures")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} (Priority {thread_priority}) "
                      f"failed to acquire resource (attempt {attempt_counter + 1})")

        attempt_counter += 1


        base_delay = 0.05
        jitter = random.uniform(0, 0.05)
        time.sleep(base_delay + jitter)


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
        print("✅ No starvation detected: low priority threads received a fair share of accesses.")

    print("=" * 60)


def main():
    """Main function to run the thread starvation simulation"""
    print("Starting Thread Starvation Simulation (fair version)")
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
        time.sleep(0.05)


    for thread_obj in active_threads:
        thread_obj.join()


    display_simulation_summary()
    print("\nSimulation completed - Fair scheduling (no starvation)")


if __name__ == "__main__":
    main()

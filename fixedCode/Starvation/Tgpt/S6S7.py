import threading
import time
import queue
import random


resource_priority_queue = queue.PriorityQueue()
resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        "successful_accesses": 0,
        "failed_attempts": 0,
        "total_wait_time": 0.0,
        "priority": priority,
        "starvation_count": 0,
    }


def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Simulate thread attempting to access a shared resource using a priority queue.

    FIXED VERSION:
    - Uses "priority aging" based on consecutive failures to prevent starvation.
    - Lower numeric priority value = higher importance.
    - Threads that keep failing get their effective priority boosted over time.
    """
    global total_resource_accesses

    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0

    print(
        f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access"
    )

    while attempt_counter < max_attempts:
        start_wait_time = time.time()






        effective_priority = thread_priority - consecutive_failures


        resource_priority_queue.put((effective_priority, time.time(), thread_id))
        resource_acquired = False
        wait_cycles = 0


        while (
            not resource_priority_queue.empty()
            and not resource_acquired
            and wait_cycles < 50
        ):
            try:
                current_priority, enqueue_time, current_thread_id = (
                    resource_priority_queue.get_nowait()
                )

                if current_thread_id == thread_id:

                    with resource_lock:
                        current_time = time.strftime(
                            "%H:%M:%S", time.localtime()
                        )
                        wait_duration = time.time() - start_wait_time

                        stats = thread_statistics[thread_id]
                        stats["successful_accesses"] += 1
                        stats["total_wait_time"] += wait_duration
                        total_resource_accesses += 1
                        consecutive_failures = 0

                        print(
                            f"[{current_time}] ✓ Thread {thread_id} "
                            f"(base priority {thread_priority}, effective {current_priority}) "
                            f"acquired resource after {wait_duration:.3f}s wait"
                        )



                        resource_usage_time = 0.15 + (thread_priority * 0.03)
                        time.sleep(resource_usage_time)

                        print(
                            f"[{current_time}] ✓ Thread {thread_id} "
                            f"(base priority {thread_priority}) released resource "
                            f"(used for {resource_usage_time:.3f}s)"
                        )
                        resource_acquired = True

                else:

                    resource_priority_queue.put(
                        (current_priority, enqueue_time, current_thread_id)
                    )
                    time.sleep(0.05 + random.uniform(0, 0.1))
                    wait_cycles += 1

            except queue.Empty:
                break


        if not resource_acquired:
            consecutive_failures += 1
            stats = thread_statistics[thread_id]
            stats["failed_attempts"] += 1
            current_time = time.strftime("%H:%M:%S", time.localtime())


            if consecutive_failures >= 3:
                stats["starvation_count"] += 1
                print(
                    f"[{current_time}] ⚠️  Thread {thread_id} (Priority {thread_priority}) "
                    f"waiting for a long time - {consecutive_failures} consecutive failures "
                    f"(effective priority now {thread_priority - consecutive_failures})"
                )
            else:
                print(
                    f"[{current_time}] ⏳ Thread {thread_id} (Priority {thread_priority}) "
                    f"failed to acquire resource (attempt {attempt_counter + 1})"
                )

        attempt_counter += 1


        base_delay = 0.1 + (thread_priority * 0.05)
        failure_penalty = min(consecutive_failures, 5) * 0.05
        inter_attempt_delay = base_delay + failure_penalty + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)


    stats = thread_statistics[thread_id]
    success_rate = (stats["successful_accesses"] / max_attempts) * 100
    avg_wait_time = stats["total_wait_time"] / max(stats["successful_accesses"], 1)

    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"   Priority: {thread_priority}")
    print(
        f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} "
        f"({success_rate:.1f}%)"
    )
    print(f"   Failed attempts: {stats['failed_attempts']}")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Starvation episodes (long waits): {stats['starvation_count']}")


def display_simulation_summary():
    """Display overall simulation statistics"""
    print("\n" + "=" * 60)
    print("THREAD STARVATION SIMULATION SUMMARY")
    print("=" * 60)
    print(f"Total resource accesses across all threads: {total_resource_accesses}")

    high_priority_accesses = sum(
        stats["successful_accesses"]
        for stats in thread_statistics.values()
        if stats["priority"] <= 2
    )
    low_priority_accesses = sum(
        stats["successful_accesses"]
        for stats in thread_statistics.values()
        if stats["priority"] > 2
    )

    print(f"High priority threads (1-2) total accesses: {high_priority_accesses}")
    print(f"Low priority threads (3+) total accesses: {low_priority_accesses}")

    total_starvation = sum(
        stats["starvation_count"] for stats in thread_statistics.values()
    )
    print(f"Total long-wait episodes detected: {total_starvation}")

    if low_priority_accesses < high_priority_accesses * 0.3:
        print(
            "⚠️  IMBALANCE: Low priority threads still significantly underserved "
            "(but true starvation should be avoided by aging)."
        )

    print("=" * 60)


def main():
    """Main function to run the thread simulation"""
    print("Starting Thread Starvation Simulation (with aging / fairness)")
    print("Lower priority numbers = Higher importance (1 = highest priority)")
    print("-" * 60)




    thread_configurations = [
        (1, "HighPrio-A"),
        (2, "HighPrio-B"),
        (4, "LowPrio-A"),
        (5, "LowPrio-B"),
        (3, "MedPrio-A"),
    ]

    active_threads = []


    for priority, thread_name in thread_configurations:
        thread_obj = threading.Thread(
            target=access_shared_resource,
            args=(priority, thread_name),
            name=f"Thread-{thread_name}",
        )
        active_threads.append(thread_obj)
        thread_obj.start()
        time.sleep(0.1)


    for thread_obj in active_threads:
        thread_obj.join()


    display_simulation_summary()
    print("\nSimulation completed - starvation should now be mitigated by aging.")


if __name__ == "__main__":
    main()

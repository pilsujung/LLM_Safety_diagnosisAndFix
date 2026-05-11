import threading
import time
import queue
import random
import itertools


resource_priority_queue = queue.PriorityQueue()
resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


AGING_INTERVAL = 0.5
MIN_PRIORITY = 1


_request_counter = itertools.count()


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
    Simulate thread attempting to access a shared resource using a priority queue
    with *aging* to prevent starvation.

    Lower priority numbers indicate higher importance (1 = highest priority).
    Aging gradually improves the priority of waiting threads so that even low
    priority threads eventually run.
    """
    global total_resource_accesses

    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0

    print(f"Thread {thread_id} (Base priority {thread_priority}) started - attempting resource access")

    while attempt_counter < max_attempts:
        start_wait_time = time.time()





        effective_priority = max(
            MIN_PRIORITY,
            thread_priority - consecutive_failures
        )

        enqueue_time = time.time()
        resource_priority_queue.put(
            (effective_priority, next(_request_counter), thread_priority, thread_id, enqueue_time)
        )

        resource_acquired = False


        while not resource_priority_queue.empty() and not resource_acquired:
            try:
                (current_eff_prio,
                 _seq,
                 base_priority,
                 current_thread_id,
                 original_enqueue_time) = resource_priority_queue.get_nowait()
            except queue.Empty:
                break

            if current_thread_id == thread_id:

                with resource_lock:
                    current_time = time.strftime('%H:%M:%S', time.localtime())
                    wait_duration = time.time() - start_wait_time


                    stats = thread_statistics[thread_id]
                    stats['successful_accesses'] += 1
                    stats['total_wait_time'] += wait_duration

                    total_resource_accesses += 1
                    consecutive_failures = 0

                    print(
                        f"[{current_time}] ✓ Thread {thread_id} "
                        f"(base {thread_priority}, effective {current_eff_prio}) "
                        f"acquired resource after {wait_duration:.3f}s wait"
                    )



                    resource_usage_time = 0.15 + (thread_priority * 0.05)
                    time.sleep(resource_usage_time)

                    print(
                        f"[{current_time}] ✓ Thread {thread_id} "
                        f"(base {thread_priority}) released resource "
                        f"(used for {resource_usage_time:.3f}s)"
                    )
                    resource_acquired = True

            else:




                waited = time.time() - original_enqueue_time
                aging_boost = int(waited / AGING_INTERVAL)
                aged_effective_priority = max(
                    MIN_PRIORITY,
                    base_priority - aging_boost
                )

                resource_priority_queue.put(
                    (aged_effective_priority, next(_request_counter),
                     base_priority, current_thread_id, original_enqueue_time)
                )

                time.sleep(0.005)


        if not resource_acquired:
            consecutive_failures += 1
            stats = thread_statistics[thread_id]
            stats['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())


            if consecutive_failures >= 3:
                stats['starvation_count'] += 1
                print(
                    f"[{current_time}] ⚠️  Thread {thread_id} (base priority {thread_priority}) "
                    f"experiencing STARVATION - {consecutive_failures} consecutive failures"
                )
            else:
                print(
                    f"[{current_time}] ⏳ Thread {thread_id} (base priority {thread_priority}) "
                    f"failed to acquire resource (attempt {attempt_counter + 1})"
                )

        attempt_counter += 1




        inter_attempt_delay = 0.1 + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)


    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)

    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"   Base priority: {thread_priority}")
    print(f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"   Failed attempts: {stats['failed_attempts']}")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Starvation episodes detected: {stats['starvation_count']}")


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
        print("✓ No significant starvation detected (aging scheduler is fairer).")

    print("=" * 60)


def main():
    """Main function to run the thread starvation simulation"""
    print("Starting Thread Starvation Simulation (with aging / anti-starvation)")
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
    print("\nSimulation completed - Starvation should now be mitigated by aging.")


if __name__ == "__main__":
    main()

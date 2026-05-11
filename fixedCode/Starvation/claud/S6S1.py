import threading
import time
import queue
import random


resource_priority_queue = queue.PriorityQueue()
resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


AGING_FACTOR = 0.1
MAX_PRIORITY_BOOST = 3

def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0
    }

def calculate_effective_priority(original_priority, wait_time):
    """
    Calculate effective priority with aging mechanism.
    The longer a thread waits, the higher its effective priority becomes.
    """

    priority_boost = min(wait_time * AGING_FACTOR, MAX_PRIORITY_BOOST)
    effective_priority = original_priority - priority_boost
    return max(effective_priority, 0)

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Simulate thread attempting to access a shared resource using priority queue.
    Lower priority numbers indicate higher importance (1 = highest priority).
    This implementation uses AGING to prevent starvation.
    """
    global total_resource_accesses

    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0

    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")

    while attempt_counter < max_attempts:
        start_wait_time = time.time()


        enqueue_time = time.time()
        effective_priority = calculate_effective_priority(thread_priority, 0)
        resource_priority_queue.put((effective_priority, enqueue_time, thread_id))
        resource_acquired = False
        wait_cycles = 0


        while not resource_priority_queue.empty() and not resource_acquired and wait_cycles < 50:
            try:
                current_priority, queue_enqueue_time, current_thread_id = resource_priority_queue.get_nowait()

                if current_thread_id == thread_id:

                    with resource_lock:
                        current_time = time.strftime('%H:%M:%S', time.localtime())
                        wait_duration = time.time() - start_wait_time
                        original_priority = thread_statistics[thread_id]['priority']


                        thread_statistics[thread_id]['successful_accesses'] += 1
                        thread_statistics[thread_id]['total_wait_time'] += wait_duration
                        total_resource_accesses += 1
                        consecutive_failures = 0


                        if original_priority != current_priority:
                            print(f"[{current_time}] ✓ Thread {thread_id} (Original Priority {original_priority} → Effective {current_priority:.2f}) acquired resource after {wait_duration:.3f}s wait")
                        else:
                            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {original_priority}) acquired resource after {wait_duration:.3f}s wait")


                        resource_usage_time = 0.15
                        time.sleep(resource_usage_time)

                        print(f"[{current_time}] ✓ Thread {thread_id} released resource (used for {resource_usage_time:.3f}s)")
                        resource_acquired = True

                else:

                    original_priority_other = thread_statistics[current_thread_id]['priority']
                    time_waited = time.time() - queue_enqueue_time
                    new_effective_priority = calculate_effective_priority(original_priority_other, time_waited)
                    

                    resource_priority_queue.put((new_effective_priority, queue_enqueue_time, current_thread_id))
                    time.sleep(0.05)
                    wait_cycles += 1

            except queue.Empty:
                break


        if not resource_acquired:
            consecutive_failures += 1
            thread_statistics[thread_id]['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())


            if consecutive_failures >= 3:
                thread_statistics[thread_id]['starvation_count'] += 1
                print(f"[{current_time}] ⚠️ Thread {thread_id} (Priority {thread_priority}) experiencing delays - {consecutive_failures} consecutive failures")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} (Priority {thread_priority}) failed to acquire resource (attempt {attempt_counter + 1})")

        attempt_counter += 1


        inter_attempt_delay = 0.1 + random.uniform(0, 0.05)
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
    print("\n" + "="*60)
    print("THREAD FAIRNESS SIMULATION SUMMARY (WITH AGING)")
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

    if low_priority_accesses > 0 and high_priority_accesses > 0:
        ratio = low_priority_accesses / high_priority_accesses
        print(f"Access ratio (low/high priority): {ratio:.2f}")
        if ratio > 0.3:
            print("✓ FAIR SCHEDULING: Low priority threads getting reasonable access!")
        else:
            print("⚠️ Some imbalance remains, but starvation is reduced")
    
    print("="*60)

def main():
    """Main function to run the thread fairness simulation"""
    print("Starting Thread Fairness Simulation with AGING")
    print("Lower priority numbers = Higher importance (1 = highest priority)")
    print("Aging mechanism: Threads gain priority the longer they wait")
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
    print("\nSimulation completed - Starvation prevented through aging!")

if __name__ == "__main__":
    main()
import threading
import time
import queue
import random


resource_priority_queue = queue.PriorityQueue()
resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}
aging_counter = {}

def initialize_thread_stats(thread_id, priority):
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0
    }
    aging_counter[thread_id] = 0

def get_effective_priority(thread_id, base_priority):
    age = aging_counter.get(thread_id, 0)
    effective_priority = base_priority - (age * 0.5)
    return max(1, effective_priority)

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    global total_resource_accesses
    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0

    print(f"Thread {thread_id} (Priority {thread_priority}) started")

    while attempt_counter < max_attempts:
        start_wait_time = time.time()
        effective_priority = get_effective_priority(thread_id, thread_priority)
        resource_priority_queue.put((effective_priority, thread_id, time.time()))
        resource_acquired = False
        wait_cycles = 0

        while not resource_priority_queue.empty() and not resource_acquired and wait_cycles < 50:
            try:
                current_priority, current_thread_id, enqueue_time = resource_priority_queue.get_nowait()

                if current_thread_id == thread_id:
                    with resource_lock:
                        current_time = time.strftime('%H:%M:%S', time.localtime())
                        wait_duration = time.time() - start_wait_time

                        thread_statistics[thread_id]['successful_accesses'] += 1
                        thread_statistics[thread_id]['total_wait_time'] += wait_duration
                        total_resource_accesses += 1
                        aging_counter[thread_id] = 0
                        consecutive_failures = 0

                    print(f"[{current_time}] ✓ Thread {thread_id} (Prio {current_priority:.1f}) acquired after {wait_duration:.3f}s")

                    resource_usage_time = 0.15 + (thread_priority * 0.05)
                    time.sleep(resource_usage_time)

                    print(f"[{current_time}] ✓ Thread {thread_id} released (used {resource_usage_time:.3f}s)")
                    resource_acquired = True

                else:
                    aging_counter[current_thread_id] = aging_counter.get(current_thread_id, 0) + 1
                    other_base_priority = thread_statistics[current_thread_id]['priority']
                    resource_priority_queue.put((get_effective_priority(current_thread_id, other_base_priority), current_thread_id, enqueue_time))
                    time.sleep(0.02 + random.uniform(0, 0.03))
                    wait_cycles += 1

            except queue.Empty:
                break

        if not resource_acquired:
            consecutive_failures += 1
            aging_counter[thread_id] = aging_counter.get(thread_id, 0) + 1
            thread_statistics[thread_id]['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())
            if consecutive_failures >= 3:
                thread_statistics[thread_id]['starvation_count'] += 1
                print(f"[{current_time}] ⚠️ Thread {thread_id} starvation risk - {consecutive_failures} fails")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} failed attempt {attempt_counter + 1}")

        attempt_counter += 1
        base_delay = 0.05 + (thread_priority * 0.02)
        failure_penalty = min(consecutive_failures * 0.05, 0.3)
        inter_attempt_delay = base_delay + failure_penalty + random.uniform(0, 0.05)
        time.sleep(inter_attempt_delay)

    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)

    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"  Priority: {thread_priority}")
    print(f"  Success: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"  Failed: {stats['failed_attempts']}")
    print(f"  Avg wait: {avg_wait_time:.3f}s")
    print(f"  Aging: {aging_counter.get(thread_id, 0)}")
    print(f"  Starvation: {stats['starvation_count']}")

def display_simulation_summary():
    print("\n" + "="*70)
    print("THREAD SCHEDULING WITH AGING (ANTI-STARVATION)")
    print("="*70)
    print(f"Total resource accesses: {total_resource_accesses}")

    high_priority_accesses = sum(stats['successful_accesses'] for stats in thread_statistics.values() if stats['priority'] <= 2)
    low_priority_accesses = sum(stats['successful_accesses'] for stats in thread_statistics.values() if stats['priority'] > 2)

    print(f"High priority (1-2): {high_priority_accesses}")
    print(f"Low priority (3+): {low_priority_accesses}")

    total_starvation = sum(stats['starvation_count'] for stats in thread_statistics.values())
    print(f"Total starvation episodes: {total_starvation}")

    fairness_ratio = low_priority_accesses / max(high_priority_accesses, 1)
    print(f"Fairness ratio (low/high): {fairness_ratio:.2f}")

    if fairness_ratio > 0.4:
        print("✅ STARVATION PREVENTED: Fair scheduling achieved!")
    else:
        print("⚠️ Some imbalance remains")

    print("="*70)

def main():
    print("Thread Scheduling with Aging (Starvation Prevention)")
    print("Lower numbers = higher priority. Aging boosts low-priority threads.")
    print("-" * 70)

    thread_configurations = [
        (1, "HighPrio-A"),
        (2, "HighPrio-B"),
        (4, "LowPrio-A"),
        (5, "LowPrio-B"),
        (3, "MedPrio-A")
    ]

    active_threads = []
    for priority, thread_name in thread_configurations:
        thread_obj = threading.Thread(target=access_shared_resource, args=(priority, thread_name))
        active_threads.append(thread_obj)
        thread_obj.start()
        time.sleep(0.05)

    for thread_obj in active_threads:
        thread_obj.join()

    display_simulation_summary()
    print("\nSimulation completed - Fairness demonstrated")

if __name__ == "__main__":
    main()

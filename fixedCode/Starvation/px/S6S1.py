import threading
import time
import queue
import random


resource_priority_queue = queue.PriorityQueue()
resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}
aging_factor = 0.3

def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0,
        'total_waiting_time': 0
    }

def calculate_effective_priority(base_priority, failures, total_wait_time):
    """
    Calculate effective priority with aging mechanism.
    Lower effective priority number = higher priority.
    Aging promotes low-priority threads that have waited longer.
    """
    aging_boost = int(failures * aging_factor + total_wait_time * 0.1)
    effective_priority = max(1, base_priority - aging_boost)
    return effective_priority

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Fixed version with aging mechanism to prevent starvation.
    Low priority threads get promoted based on wait time and failures.
    """
    global total_resource_accesses

    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0
    total_waiting_time = 0

    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")

    while attempt_counter < max_attempts:
        start_wait_time = time.time()
        stats = thread_statistics[thread_id]
        

        effective_priority = calculate_effective_priority(
            thread_priority, stats['failed_attempts'], stats['total_waiting_time']
        )
        

        enqueue_time = time.time()
        resource_priority_queue.put((effective_priority, enqueue_time, thread_id))
        resource_acquired = False
        wait_cycles = 0
        cycle_start = time.time()


        while not resource_priority_queue.empty() and not resource_acquired and wait_cycles < 100:
            try:
                curr_priority, curr_enqueue_time, curr_thread_id = resource_priority_queue.get_nowait()
                
                if curr_thread_id == thread_id:

                    with resource_lock:
                        current_time = time.strftime('%H:%M:%S', time.localtime())
                        wait_duration = time.time() - start_wait_time


                        stats['successful_accesses'] += 1
                        stats['total_wait_time'] += wait_duration
                        stats['total_waiting_time'] += wait_duration
                        total_resource_accesses += 1
                        consecutive_failures = 0

                        print(f"[{current_time}] ✓ Thread {thread_id} (P:{curr_priority}) acquired after {wait_duration:.3f}s")


                        resource_usage_time = 0.15 + (thread_priority * 0.03)
                        time.sleep(resource_usage_time)

                        print(f"[{current_time}] ✓ Thread {thread_id} released (used {resource_usage_time:.3f}s)")
                        resource_acquired = True

                else:

                    resource_priority_queue.put((curr_priority, curr_enqueue_time, curr_thread_id))
                    

                    time.sleep(0.01)
                    wait_cycles += 1
                    
            except queue.Empty:
                break


        cycle_wait = time.time() - cycle_start
        stats['total_waiting_time'] += cycle_wait

        if not resource_acquired:
            consecutive_failures += 1
            stats['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())


            if consecutive_failures >= 3:
                stats['starvation_count'] += 1
                print(f"[{current_time}] ⚠️ Thread {thread_id} (P:{thread_priority}) STARVATION - {consecutive_failures} fails (effective P:{calculate_effective_priority(thread_priority, stats['failed_attempts'], stats['total_waiting_time'])})")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} failed attempt {attempt_counter + 1}")

        attempt_counter += 1


        base_delay = max(0.05, 0.1 - (stats['failed_attempts'] * 0.01))
        inter_attempt_delay = base_delay + random.uniform(0, 0.05)
        time.sleep(inter_attempt_delay)


    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    final_effective_priority = calculate_effective_priority(thread_priority, stats['failed_attempts'], stats['total_waiting_time'])

    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"  Base Priority: {thread_priority} → Final Effective: {final_effective_priority}")
    print(f"  Success: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"  Failures: {stats['failed_attempts']}, Starvation: {stats['starvation_count']}")
    print(f"  Avg wait: {avg_wait_time:.3f}s, Total wait time: {stats['total_waiting_time']:.1f}s")

def display_simulation_summary():
    """Display overall simulation statistics"""
    print("\n" + "="*70)
    print("ANTI-STARVATION SIMULATION SUMMARY (with AGING)")
    print("="*70)
    print(f"Total resource accesses: {total_resource_accesses}")


    high_prio_accesses = sum(s['successful_accesses'] for s in thread_statistics.values() if s['priority'] <= 2)
    med_prio_accesses = sum(s['successful_accesses'] for s in thread_statistics.values() if s['priority'] == 3)
    low_prio_accesses = sum(s['successful_accesses'] for s in thread_statistics.values() if s['priority'] > 3)

    print(f"High priority (1-2): {high_prio_accesses} accesses")
    print(f"Medium priority (3):  {med_prio_accesses} accesses") 
    print(f"Low priority (4-5):   {low_prio_accesses} accesses")

    total_starvation = sum(s['starvation_count'] for s in thread_statistics.values())
    fairness_ratio = low_prio_accesses / max(high_prio_accesses, 1)
    
    print(f"Total starvation episodes: {total_starvation}")
    print(f"Fairness ratio (low/high): {fairness_ratio:.2f}")
    
    if fairness_ratio > 0.4:
        print("✅ STARVATION PREVENTED: Low priority threads got fair access!")
    else:
        print("⚠️ Some starvation still detected")

    print("="*70)

def main():
    """Main function with anti-starvation simulation"""
    print("ANTI-STARVATION Thread Scheduling Simulation")
    print("Lower priority = Higher importance | AGING mechanism prevents starvation")
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
    print("\n✅ Simulation completed - Starvation prevented via AGING!")

if __name__ == "__main__":
    main()

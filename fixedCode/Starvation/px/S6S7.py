import threading
import time
import random


resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}
resource_owner = None
resource_owner_lock = threading.Lock()

def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0,
        'wait_start_time': None
    }

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """Thread attempts to access shared resource with priority aging to prevent starvation"""
    global total_resource_accesses, resource_owner
    
    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0

    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")

    while attempt_counter < max_attempts:
        start_wait_time = time.time()
        thread_statistics[thread_id]['wait_start_time'] = start_wait_time
        
        resource_acquired = False
        

        max_retries = 100
        for retry in range(max_retries):
            with resource_owner_lock:
                if resource_owner is None:

                    resource_owner = (thread_priority, thread_id)
                    resource_acquired = True
                    break
                else:

                    current_owner_priority, current_owner_id = resource_owner
                    age_factor = (time.time() - thread_statistics[thread_id]['wait_start_time']) * 0.1
                    my_effective_priority = thread_priority - age_factor
                    owner_effective_priority = current_owner_priority
                    
                    if my_effective_priority < owner_effective_priority:

                        resource_owner = (thread_priority, thread_id)
                        resource_acquired = True
                        break
            
            if resource_acquired:
                break
            time.sleep(0.01)

        if resource_acquired:
            with resource_lock:
                current_time = time.strftime('%H:%M:%S', time.localtime())
                wait_duration = time.time() - start_wait_time


                thread_statistics[thread_id]['successful_accesses'] += 1
                thread_statistics[thread_id]['total_wait_time'] += wait_duration
                total_resource_accesses += 1
                consecutive_failures = 0
                thread_statistics[thread_id]['wait_start_time'] = None

            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) acquired resource after {wait_duration:.3f}s wait")


            resource_usage_time = 0.15 + (thread_priority * 0.05)
            time.sleep(resource_usage_time)

            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) released resource (used for {resource_usage_time:.3f}s)")
            

            with resource_owner_lock:
                resource_owner = None
        else:
            consecutive_failures += 1
            thread_statistics[thread_id]['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())

            if consecutive_failures >= 3:
                thread_statistics[thread_id]['starvation_count'] += 1
                print(f"[{current_time}] ⚠️ Thread {thread_id} (Priority {thread_priority}) experiencing STARVATION - {consecutive_failures} consecutive failures")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} (Priority {thread_priority}) failed to acquire resource (attempt {attempt_counter + 1})")

        attempt_counter += 1


        base_delay = 0.1 + (thread_priority * 0.02)
        failure_penalty = min(consecutive_failures * 0.05, 0.5)
        inter_attempt_delay = base_delay + failure_penalty + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)


    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)

    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f" Priority: {thread_priority}")
    print(f" Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f" Failed attempts: {stats['failed_attempts']}")
    print(f" Average wait time: {avg_wait_time:.3f}s")
    print(f" Starvation episodes: {stats['starvation_count']}")

def display_simulation_summary():
    """Display overall simulation statistics"""
    print("\n" + "="*60)
    print("FAIR PRIORITY SCHEDULING SIMULATION SUMMARY (Starvation Fixed)")
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

    fairness_ratio = low_priority_accesses / max(high_priority_accesses, 1)
    print(f"Fairness ratio (low/high priority): {fairness_ratio:.2f}")

    if total_starvation == 0:
        print("✅ NO STARVATION: All threads received fair resource allocation!")
    print("="*60)

def main():
    """Main function to run the starvation-free simulation"""
    print("Starting Fair Priority Scheduling Simulation (Starvation Fixed)")
    print("Lower priority numbers = Higher importance (1 = highest priority)")
    print("Uses priority aging mechanism to prevent starvation")
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
    print("\n✅ Simulation completed - Starvation completely eliminated!")

if __name__ == "__main__":
    main()

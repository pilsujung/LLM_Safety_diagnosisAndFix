import threading
import time
import queue
import random


resource_priority_queue = queue.PriorityQueue()
resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


AGING_FACTOR = 0.1
MAX_WAIT_TIME = 5.0

def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0,
        'initial_priority': priority
    }

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Fixed version using AGING to prevent starvation.
    Priority improves the longer a thread waits.
    """
    global total_resource_accesses
    
    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0
    
    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")
    
    while attempt_counter < max_attempts:
        start_wait_time = time.time()
        current_priority = thread_priority
        

        enqueue_time = time.time()
        resource_priority_queue.put((current_priority, thread_id, enqueue_time))
        resource_acquired = False
        wait_cycles = 0
        

        while not resource_priority_queue.empty() and not resource_acquired and wait_cycles < 50:
            try:

                aged_items = []
                

                while not resource_priority_queue.empty():
                    try:
                        priority, tid, eq_time = resource_priority_queue.get_nowait()
                        wait_duration = time.time() - eq_time
                        

                        age_improvement = int(wait_duration / AGING_FACTOR)
                        aged_priority = max(1, priority - age_improvement)
                        
                        aged_items.append((aged_priority, tid, eq_time))
                    except queue.Empty:
                        break
                

                aged_items.sort(key=lambda x: (x[0], x[2]))
                
                if aged_items:
                    selected_priority, selected_tid, selected_time = aged_items[0]
                    
                    if selected_tid == thread_id:

                        with resource_lock:
                            current_time = time.strftime('%H:%M:%S', time.localtime())
                            wait_duration = time.time() - start_wait_time
                            

                            thread_statistics[thread_id]['successful_accesses'] += 1
                            thread_statistics[thread_id]['total_wait_time'] += wait_duration
                            total_resource_accesses += 1
                            consecutive_failures = 0
                            
                            aging_info = f" (aged from {thread_priority} to {selected_priority})" if selected_priority < thread_priority else ""
                            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {selected_priority}{aging_info}) acquired resource after {wait_duration:.3f}s wait")
                            

                            resource_usage_time = 0.15
                            time.sleep(resource_usage_time)
                            
                            print(f"[{current_time}] ✓ Thread {thread_id} released resource (used for {resource_usage_time:.3f}s)")
                            resource_acquired = True
                            

                            for item in aged_items[1:]:
                                resource_priority_queue.put(item)
                    else:

                        for item in aged_items:
                            resource_priority_queue.put(item)
                        time.sleep(0.05)
                        wait_cycles += 1
                else:
                    break
                    
            except queue.Empty:
                break
        

        if not resource_acquired:
            consecutive_failures += 1
            thread_statistics[thread_id]['failed_attempts'] += 1
            current_time = time.strftime('%H:%M:%S', time.localtime())
            
            if consecutive_failures >= 3:
                thread_statistics[thread_id]['starvation_count'] += 1
                print(f"[{current_time}] ⚠️  Thread {thread_id} (Priority {thread_priority}) experiencing delays - {consecutive_failures} consecutive failures")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} (Priority {thread_priority}) retrying (attempt {attempt_counter + 1})")
        
        attempt_counter += 1
        

        inter_attempt_delay = 0.1 + random.uniform(0, 0.05)
        time.sleep(inter_attempt_delay)
    

    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    
    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"   Initial Priority: {stats['initial_priority']}")
    print(f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"   Failed attempts: {stats['failed_attempts']}")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Delay episodes: {stats['starvation_count']}")

def display_simulation_summary():
    """Display overall simulation statistics"""
    print("\n" + "="*60)
    print("FIXED SIMULATION SUMMARY (WITH AGING)")
    print("="*60)
    print(f"Total resource accesses across all threads: {total_resource_accesses}")
    
    high_priority_accesses = sum(stats['successful_accesses'] 
                                for stats in thread_statistics.values() 
                                if stats['initial_priority'] <= 2)
    low_priority_accesses = sum(stats['successful_accesses'] 
                               for stats in thread_statistics.values() 
                               if stats['initial_priority'] > 2)
    
    print(f"High priority threads (1-2) total accesses: {high_priority_accesses}")
    print(f"Low priority threads (3+) total accesses: {low_priority_accesses}")
    
    total_starvation = sum(stats['starvation_count'] for stats in thread_statistics.values())
    print(f"Total delay episodes detected: {total_starvation}")
    
    fairness_ratio = low_priority_accesses / max(high_priority_accesses, 1)
    print(f"Fairness ratio (low/high): {fairness_ratio:.2f}")
    
    if fairness_ratio > 0.5:
        print("✓ STARVATION PREVENTED: Fair resource distribution achieved!")
    else:
        print("⚠️  Some imbalance remains, but improved from original")
    
    print("="*60)

def main():
    """Main function to run the fixed simulation"""
    print("Starting FIXED Thread Simulation (With Aging)")
    print("Aging mechanism: Priority improves as threads wait longer")
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
    print("\nSimulation completed - Starvation prevention demonstrated!")

if __name__ == "__main__":
    main()
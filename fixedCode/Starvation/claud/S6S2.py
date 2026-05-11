import threading
import time
import queue
import random


resource_fifo_queue = queue.Queue()
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
    Simulate thread attempting to access a shared resource using FIFO queue.
    This implementation prevents starvation by ensuring fair access regardless of priority.
    """
    global total_resource_accesses
    
    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    consecutive_failures = 0
    
    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")
    
    while attempt_counter < max_attempts:
        start_wait_time = time.time()
        

        resource_fifo_queue.put((thread_id, time.time()))
        resource_acquired = False
        

        while not resource_acquired:
            try:
                current_thread_id, enqueue_time = resource_fifo_queue.get(timeout=1.0)
                
                if current_thread_id == thread_id:

                    with resource_lock:
                        current_time = time.strftime('%H:%M:%S', time.localtime())
                        wait_duration = time.time() - start_wait_time
                        

                        thread_statistics[thread_id]['successful_accesses'] += 1
                        thread_statistics[thread_id]['total_wait_time'] += wait_duration
                        total_resource_accesses += 1
                        consecutive_failures = 0
                        
                        print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) acquired resource after {wait_duration:.3f}s wait")
                        

                        resource_usage_time = 0.1
                        time.sleep(resource_usage_time)
                        
                        print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) released resource (used for {resource_usage_time:.3f}s)")
                        resource_acquired = True
                        
                else:

                    resource_fifo_queue.put((current_thread_id, enqueue_time))
                    time.sleep(0.01)
                    
            except queue.Empty:

                consecutive_failures += 1
                thread_statistics[thread_id]['failed_attempts'] += 1
                break
        

        if not resource_acquired:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            

            if consecutive_failures >= 3:
                thread_statistics[thread_id]['starvation_count'] += 1
                print(f"[{current_time}] ⚠️  Thread {thread_id} (Priority {thread_priority}) timeout - {consecutive_failures} consecutive failures")
            else:
                print(f"[{current_time}] ⏳ Thread {thread_id} (Priority {thread_priority}) timeout (attempt {attempt_counter + 1})")
        
        attempt_counter += 1
        

        inter_attempt_delay = 0.05 + random.uniform(0, 0.05)
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
    print("FAIR THREAD SCHEDULING SIMULATION SUMMARY")
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
    

    if high_priority_accesses > 0 and low_priority_accesses > 0:
        fairness_ratio = low_priority_accesses / high_priority_accesses
        print(f"Fairness ratio (low/high priority): {fairness_ratio:.2f}")
        
        if fairness_ratio > 0.7:
            print("✓ FAIR SCHEDULING: All threads have reasonable access to resources!")
        elif fairness_ratio > 0.3:
            print("⚠️  Moderate imbalance detected")
        else:
            print("⚠️  STARVATION DETECTED: Low priority threads significantly underserved!")
    
    print("="*60)

def main():
    """Main function to run the fair thread scheduling simulation"""
    print("Starting Fair Thread Scheduling Simulation (FIFO Queue)")
    print("All threads get equal access regardless of priority")
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
    print("\nSimulation completed - Fair scheduling demonstrated")

if __name__ == "__main__":
    main()
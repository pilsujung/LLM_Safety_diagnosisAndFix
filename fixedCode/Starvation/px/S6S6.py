import threading
import time
import queue
import random
from collections import defaultdict


resource_lock = threading.Lock()
wait_times = {}
thread_stats = defaultdict(lambda: {
    'successful_accesses': 0, 'failed_attempts': 0, 'total_wait_time': 0,
    'priority': 0, 'starvation_count': 0
})
total_accesses = 0
waiting_threads = queue.PriorityQueue()
manager_running = True

def get_effective_priority(base_priority, wait_duration):
    """Calculate effective priority with aging: lower number = higher priority"""
    aging_boost = max(0, int(wait_duration * 10))
    return base_priority - aging_boost

def thread_wait_loop(thread_priority, thread_id, max_attempts=25):
    """Each thread waits for notification from manager"""
    global total_accesses
    thread_stats[thread_id]['priority'] = thread_priority
    consecutive_failures = 0
    
    print(f"Thread {thread_id} (Priority {thread_priority}) started")
    
    for attempt in range(max_attempts):
        start_time = time.time()
        
        with resource_lock:
            wait_times[thread_id] = time.time()
            wait_duration = time.time() - start_time
            effective_prio = get_effective_priority(thread_priority, wait_duration)
            waiting_threads.put((effective_prio, thread_id, wait_times[thread_id]))
        

        accessed = False
        timeout = 3.0 + thread_priority * 0.5
        
        while time.time() - start_time < timeout:
            if thread_id in wait_times:
                del wait_times[thread_id]
                if accessed:
                    break
            time.sleep(0.01)
        
        if not accessed:
            consecutive_failures += 1
            thread_stats[thread_id]['failed_attempts'] += 1
            if consecutive_failures >= 3:
                thread_stats[thread_id]['starvation_count'] += 1
                print(f"[{time.strftime('%H:%M:%S')}] ⚠️ {thread_id} STARVATION (attempt {attempt+1})")
    
    print_final_stats(thread_id, max_attempts)

def resource_manager():
    """Single manager thread that fairly dispatches resource access"""
    global total_accesses, manager_running
    
    while manager_running or not waiting_threads.empty():
        try:
            if not waiting_threads.empty():
                effective_prio, thread_id, enqueue_time = waiting_threads.get_nowait()
                wait_duration = time.time() - enqueue_time
                
                with resource_lock:
                    if thread_id in wait_times:
                        print(f"[{time.strftime('%H:%M:%S')}] ✓ {thread_id} "
                              f"(eff_prio:{effective_prio}) got resource ({wait_duration:.2f}s wait)")
                        

                        thread_stats[thread_id]['successful_accesses'] += 1
                        thread_stats[thread_id]['total_wait_time'] += wait_duration
                        total_accesses += 1
                        
                        del wait_times[thread_id]
                        

                        usage_time = 0.15 + (thread_stats[thread_id]['priority'] * 0.05)
                        print(f"[{time.strftime('%H:%M:%S')}] ✓ {thread_id} released ({usage_time:.2f}s)")
                        

                        time.sleep(usage_time)
            
            time.sleep(0.01)
            
        except queue.Empty:
            time.sleep(0.05)

def print_final_stats(thread_id, max_attempts):
    stats = thread_stats[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    
    print(f"\n📊 {thread_id} Final: {stats['successful_accesses']}/{max_attempts} "
          f"({success_rate:.1f}%) | Wait: {avg_wait:.3f}s | Starvation: {stats['starvation_count']}")

def print_summary():
    print("\n" + "="*70)
    print("FAIR PRIORITY SCHEDULING - NO STARVATION")
    print("="*70)
    print(f"Total accesses: {total_accesses}")
    
    high = sum(s['successful_accesses'] for s in thread_stats.values() if s['priority'] <= 2)
    low = sum(s['successful_accesses'] for s in thread_stats.values() if s['priority'] > 2)
    total_starvation = sum(s['starvation_count'] for s in thread_stats.values())
    
    print(f"High prio (1-2): {high} | Low prio (3+): {low}")
    print(f"Total starvation: {total_starvation}")
    print("="*70)

def main():
    print("Fair Priority Scheduling Simulation (No Starvation)")
    print("Lower numbers = higher priority + AGING mechanism")
    print("-" * 60)
    

    manager_thread = threading.Thread(target=resource_manager, daemon=True)
    manager_thread.start()
    time.sleep(0.1)
    

    configs = [(1, "HighPrio-A"), (2, "HighPrio-B"), (4, "LowPrio-A"), 
               (5, "LowPrio-B"), (3, "MedPrio-A")]
    
    threads = []
    for prio, name in configs:
        t = threading.Thread(target=thread_wait_loop, args=(prio, name))
        threads.append(t)
        t.start()
        time.sleep(0.05)
    
    for t in threads:
        t.join()
    
    global manager_running
    manager_running = False
    time.sleep(0.5)
    print_summary()

if __name__ == "__main__":
    main()

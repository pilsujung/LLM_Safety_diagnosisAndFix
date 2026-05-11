import threading
import time
import queue
import random
from collections import defaultdict


resource_lock = threading.Lock()
waiting_threads = {}
thread_stats = defaultdict(lambda: {
    'successful_accesses': 0, 'failed_attempts': 0, 'total_wait_time': 0,
    'priority': 0, 'starvation_count': 0, 'wait_start': None
})
total_resource_accesses = 0
wait_lock = threading.Lock()

def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    global total_resource_accesses
    
    thread_stats[thread_id]['priority'] = thread_priority
    consecutive_failures = 0
    
    print(f"Thread {thread_id} (Priority {thread_priority}) started")
    
    for attempt in range(max_attempts):
        start_time = time.time()
        

        age = time.time()
        entry = (thread_priority + (age * 0.001), age, thread_id)
        thread_stats[thread_id]['wait_start'] = start_time
        
        with wait_lock:
            waiting_threads[entry] = waiting_threads.get(entry, 0) + 1
        
        print(f"[{time.strftime('%H:%M:%S')}] Thread {thread_id} enqueued (effective prio: {entry[0]:.3f})")
        

        resource_lock.acquire(timeout=1.0)
        if not resource_lock.locked():
            with wait_lock:
                waiting_threads.pop(entry, None)
            continue
            
        try:

            if waiting_threads:
                best_entry = min(waiting_threads.keys())
                best_thread = best_entry[2]
                
                if best_thread == thread_id:

                    with wait_lock:
                        waiting_threads.pop(best_entry, None)
                    
                    wait_duration = time.time() - start_time
                    current_time = time.strftime('%H:%M:%S', time.localtime())
                    
                    thread_stats[thread_id]['successful_accesses'] += 1
                    thread_stats[thread_id]['total_wait_time'] += wait_duration
                    total_resource_accesses += 1
                    consecutive_failures = 0
                    
                    print(f"[{current_time}] ✓ {thread_id} (P{thread_priority}) acquired after {wait_duration:.3f}s")
                    

                    usage_time = 0.15 + (thread_priority * 0.05)
                    time.sleep(usage_time)
                    
                    print(f"[{current_time}] ✓ {thread_id} released (used {usage_time:.3f}s)")
                else:

                    with wait_lock:
                        waiting_threads.pop(entry, None)
                    consecutive_failures += 1
                    thread_stats[thread_id]['failed_attempts'] += 1
                    
                    if consecutive_failures >= 3:
                        thread_stats[thread_id]['starvation_count'] += 1
                        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ {thread_id} STARVATION ({consecutive_failures})")
        finally:
            resource_lock.release()
        

        delay = 0.05 + (consecutive_failures * 0.02) + random.uniform(0, 0.05)
        time.sleep(delay)
    

    stats = thread_stats[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    
    print(f"\n📊 {thread_id} (P{thread_priority}): {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%) "
          f"wait:{avg_wait:.3f}s starve:{stats['starvation_count']}")

def display_summary():
    print("\n" + "="*70)
    print("ANTI-STARVATION PRIORITY SCHEDULER - AGING IMPLEMENTATION")
    print("="*70)
    print(f"Total accesses: {total_resource_accesses}")
    
    high = sum(s['successful_accesses'] for s in thread_stats.values() if s['priority'] <= 2)
    low = sum(s['successful_accesses'] for s in thread_stats.values() if s['priority'] > 2)
    total_starve = sum(s['starvation_count'] for s in thread_stats.values())
    
    print(f"High prio (1-2): {high} | Low prio (3+): {low} | Ratio: {low/high:.2f}x")
    print(f"Total starvation events: {total_starve}")
    
    if low >= high * 0.4 and total_starve == 0:
        print("✅ STARVATION ELIMINATED - Fair scheduling achieved!")
    print("="*70)

def main():
    print("Anti-Starvation Priority Scheduler (Priority + Aging)")
    print("Lower numbers = higher priority, aging prevents starvation")
    print("-"*70)
    
    configs = [(1, "HighPrio-A"), (2, "HighPrio-B"), (4, "LowPrio-A"), 
               (5, "LowPrio-B"), (3, "MedPrio-A")]
    
    threads = []
    for prio, name in configs:
        t = threading.Thread(target=access_shared_resource, args=(prio, name))
        threads.append(t)
        t.start()
        time.sleep(0.02)
    
    for t in threads:
        t.join()
    
    display_summary()

if __name__ == "__main__":
    main()

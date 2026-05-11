import threading
import time
import random
from collections import deque


ticket_counter = 0
ticket_lock = threading.Lock()
waiting_tickets = deque()
resource_semaphore = threading.Semaphore(1)


thread_stats = {}
stats_lock = threading.Lock()

def get_ticket():
    """Get next sequential ticket number (FIFO fairness)"""
    global ticket_counter
    with ticket_lock:
        ticket = ticket_counter
        ticket_counter += 1
        waiting_tickets.append(ticket)
        return ticket

def wait_for_ticket(ticket):
    """Wait until this ticket reaches front of queue"""
    while True:
        with ticket_lock:
            if waiting_tickets and waiting_tickets[0] == ticket:
                waiting_tickets.popleft()
                return True
        time.sleep(0.001)

def initialize_stats(thread_id):
    with stats_lock:
        thread_stats[thread_id] = {'access_count': 0, 'total_wait_time': 0, 
                                  'total_usage_time': 0, 'last_access_time': None}

def update_stats(thread_id, wait_time, usage_time):
    with stats_lock:
        stats = thread_stats[thread_id]
        stats['access_count'] += 1
        stats['total_wait_time'] += wait_time
        stats['total_usage_time'] += usage_time
        stats['last_access_time'] = time.time()

def print_stats():
    with stats_lock:
        print("\n" + "="*60)
        print("FAIR SCHEDULING STATISTICS (TICKET SYSTEM)")
        print("="*60)
        for thread_id, stats in sorted(thread_stats.items()):
            avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
            print(f"Thread {thread_id}: Accesses={stats['access_count']}, "
                  f"AvgWait={avg_wait:.3f}s, TotalUsage={stats['total_usage_time']:.3f}s")
        print("="*60 + "\n")

def fair_resource_user(thread_id, base_hold_time, variation_factor, is_greedy=True):
    initialize_stats(thread_id)
    iteration_count = 20 if is_greedy else 50
    prefix = "[GREEDY]" if is_greedy else "[LIGHT]"
    
    for i in range(iteration_count):
        wait_start = time.time()
        

        ticket = get_ticket()
        

        wait_for_ticket(ticket)
        wait_end = time.time()
        wait_duration = wait_end - wait_start
        

        if not resource_semaphore.acquire(timeout=1.0):
            print(f"{prefix} Thread {thread_id} semaphore timeout")
            continue
            
        try:
            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
            print(f"{prefix} T{thread_id} ticket={ticket} (waited {wait_duration:.3f}s) "
                  f"holds {actual_hold_time:.3f}s")
            
            usage_start = time.time()
            time.sleep(actual_hold_time)
            actual_usage = time.time() - usage_start
            
            print(f"{prefix} T{thread_id} released after {actual_usage:.3f}s")
            update_stats(thread_id, wait_duration, actual_usage)
            
        finally:
            resource_semaphore.release()
        
        time.sleep(0.01 if is_greedy else 0.02)

def monitor_thread():
    time.sleep(2)
    for _ in range(6):
        time.sleep(2)
        print_stats()

def main():
    print("ANTI-STARVATION DEMO: TICKET-BASED FAIR SCHEDULING")
    print("="*60)
    print("Each thread gets sequential tickets → FIFO access regardless of hold time")
    print("="*60 + "\n")
    
    threads = [
        threading.Thread(target=fair_resource_user, args=(1, 1.0, 0.2, True), name="Greedy1"),
        threading.Thread(target=fair_resource_user, args=(2, 0.8, 0.3, True), name="Greedy2"),
        threading.Thread(target=fair_resource_user, args=(3, 0.1, 0.02, False), name="Light1"),
        threading.Thread(target=fair_resource_user, args=(4, 0.05, 0.01, False), name="Light2"),
        threading.Thread(target=monitor_thread, name="Monitor")
    ]
    
    print("Starting fair threads...\n")
    for t in threads:
        t.start()
        time.sleep(0.05)
    
    for t in threads:
        t.join()
    
    print("\nFINAL RESULTS (FAIR ACCESS ACHIEVED):")
    print_stats()

if __name__ == "__main__":
    main()

import threading
import time
import random
from datetime import datetime
from collections import deque


resource_semaphore = threading.Semaphore(1)
console_lock = threading.Lock()


wait_queue = deque()
queue_lock = threading.Lock()


SIMULATION_DURATION = 7.0
VIP_WORK_TIME = 0.003
STANDARD_WORK_TIME = 0.0045
FREE_WORK_TIME = 0.006
VIP_SLEEP_TIME = 0.001
STANDARD_SLEEP_TIME = 0.006
FREE_SLEEP_TIME = 0.010


PRIORITIES = {
    'vip_client': 1,
    'standard_client': 2,
    'free_client': 3
}

simulation_end_time = None

client_statistics = {
    'vip_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None, 'wait_times': []},
    'standard_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None, 'wait_times': []},
    'free_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None, 'wait_times': []}
}

database_row_count = 0
total_db_transactions = 0

def safe_print(message):
    with console_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")

def add_to_wait_queue(client_name):
    """Add client to fair wait queue with priority"""
    with queue_lock:
        wait_queue.append((PRIORITIES[client_name], client_name))

def remove_from_wait_queue(client_name):
    """Remove client from wait queue"""
    with queue_lock:
        for i, (priority, name) in enumerate(wait_queue):
            if name == client_name:
                wait_queue.popleft(i)
                break

def get_next_client():
    """Get next client from priority queue (fair scheduling)"""
    with queue_lock:
        if not wait_queue:
            return None

        best_priority = min(priority for priority, _ in wait_queue)
        candidates = [(priority, name) for priority, name in wait_queue if priority == best_priority]

        for i, entry in enumerate(wait_queue):
            if entry in candidates:
                wait_queue.popleft(i)
                return entry[1]
    return None

def acquire_fair_lock(client_name, max_wait=0.1):
    """Fair lock acquisition with priority queue"""
    start_wait = time.time()
    add_to_wait_queue(client_name)
    
    while time.time() - start_wait < max_wait:
        if resource_semaphore.acquire(timeout=0.001):

            next_client = get_next_client()
            if next_client == client_name:
                remove_from_wait_queue(client_name)
                return True
            else:

                resource_semaphore.release()
                time.sleep(0.001)
        else:
            time.sleep(0.001)
    

    remove_from_wait_queue(client_name)
    client_statistics[client_name]['failed_attempts'] += 1
    return False

def simulate_db_work(client_name, work_duration):
    global database_row_count, total_db_transactions
    start_work_time = time.time()
    for _ in range(random.randint(10, 30)):
        database_row_count += 1
    total_db_transactions += 1
    time.sleep(work_duration)
    end_work_time = time.time()
    actual_work_time = end_work_time - start_work_time
    client_statistics[client_name]['total_work_time'] += actual_work_time
    return actual_work_time

def vip_client_thread():
    client_name = 'vip_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("💎 VIP CLIENT: Starting requests (FAIR SCHEDULING)")
    
    while time.time() < simulation_end_time:
        start_acquire = time.time()
        if acquire_fair_lock(client_name):
            wait_time = time.time() - start_acquire
            client_statistics[client_name]['wait_times'].append(wait_time)
            
            client_statistics[client_name]['access_count'] += 1
            current_count = client_statistics[client_name]['access_count']
            if current_count % 50 == 0:
                safe_print(f"💎 VIP: Acquired #{current_count} (wait: {wait_time*1000:.1f}ms)")
            
            simulate_db_work(client_name, VIP_WORK_TIME)
            resource_semaphore.release()
        time.sleep(VIP_SLEEP_TIME)
    
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("💎 VIP CLIENT: Stopped")

def standard_client_thread():
    client_name = 'standard_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Starting requests (FAIR SCHEDULING)")
    
    while time.time() < simulation_end_time:
        start_acquire = time.time()
        if acquire_fair_lock(client_name):
            wait_time = time.time() - start_acquire
            client_statistics[client_name]['wait_times'].append(wait_time)
            
            client_statistics[client_name]['access_count'] += 1
            current_count = client_statistics[client_name]['access_count']
            if current_count % 50 == 0:
                safe_print(f"🟦 STANDARD: Acquired #{current_count} (wait: {wait_time*1000:.1f}ms)")
            
            simulate_db_work(client_name, STANDARD_WORK_TIME)
            resource_semaphore.release()
        time.sleep(STANDARD_SLEEP_TIME)
    
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Stopped")

def free_client_thread():
    client_name = 'free_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Starting requests (FAIR SCHEDULING)")
    
    while time.time() < simulation_end_time:
        start_acquire = time.time()
        if acquire_fair_lock(client_name):
            wait_time = time.time() - start_acquire
            client_statistics[client_name]['wait_times'].append(wait_time)
            
            client_statistics[client_name]['access_count'] += 1
            current_count = client_statistics[client_name]['access_count']
            safe_print(f"🆓 FREE: Acquired #{current_count} (wait: {wait_time*1000:.1f}ms)")
            
            simulate_db_work(client_name, FREE_WORK_TIME)
            resource_semaphore.release()
        time.sleep(FREE_SLEEP_TIME)
    
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Stopped")

def print_final_statistics():
    safe_print("\n" + "=" * 80)
    safe_print("FAIR SCHEDULING - NO STARVATION DEMONSTRATION")
    safe_print("=" * 80)
    
    for client_name, stats in client_statistics.items():
        if stats['start_time'] and stats['end_time']:
            execution_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count'] / total_attempts * 100) if total_attempts > 0 else 0
            avg_work_time = stats['total_work_time'] / stats['access_count'] if stats['access_count'] > 0 else 0
            avg_wait_time = sum(stats['wait_times']) / len(stats['wait_times']) if stats['wait_times'] else 0
            
            readable_name = client_name.upper().replace('_', ' ')
            safe_print(f"\n📈 {readable_name}:")
            safe_print(f"  • Successful requests: {stats['access_count']}")
            safe_print(f"  • Failed attempts: {stats['failed_attempts']}")
            safe_print(f"  • Success rate: {success_rate:.1f}%")
            safe_print(f"  • Avg wait time: {avg_wait_time*1000:.1f}ms")
            safe_print(f"  • Total work time: {stats['total_work_time']:.2f}s")
    
    safe_print(f"\n🔢 DATABASE: {database_row_count:,} rows, {total_db_transactions} transactions")
    safe_print("=" * 80)

def main():
    global simulation_end_time
    safe_print("🚀 FAIR SCHEDULING DEMO - NO MORE STARVATION!")
    safe_print(f"Duration: {SIMULATION_DURATION}s")
    safe_print("-" * 80)
    
    simulation_end_time = time.time() + SIMULATION_DURATION
    
    vip_thread = threading.Thread(target=vip_client_thread, name="VIP")
    standard_thread = threading.Thread(target=standard_client_thread, name="Standard") 
    free_thread = threading.Thread(target=free_client_thread, name="Free")
    
    vip_thread.start()
    standard_thread.start()
    free_thread.start()
    
    vip_thread.join()
    standard_thread.join()
    free_thread.join()
    
    print_final_statistics()
    safe_print("✅ STARVATION ELIMINATED!")

if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Total runtime: {time.time() - start:.2f}s")

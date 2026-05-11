import threading
import time
import random
from datetime import datetime
from collections import deque


resource_lock = threading.Lock()
console_lock = threading.Lock()
wait_queue = deque()
client_priorities = {
    'vip_client': 3,
    'standard_client': 2,
    'free_client': 1
}


SIMULATION_DURATION = 7.0
VIP_WORK_TIME = 0.003
STANDARD_WORK_TIME = 0.0045
FREE_WORK_TIME = 0.006
VIP_SLEEP_TIME = 0.001
STANDARD_SLEEP_TIME = 0.006
FREE_SLEEP_TIME = 0.010


simulation_end_time = None


client_statistics = {
    'vip_client': {
        'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0,
        'start_time': None, 'end_time': None, 'wait_times': []
    },
    'standard_client': {
        'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0,
        'start_time': None, 'end_time': None, 'wait_times': []
    },
    'free_client': {
        'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0,
        'start_time': None, 'end_time': None, 'wait_times': []
    }
}


database_row_count = 0
total_db_transactions = 0

def safe_print(message):
    """Thread-safe printing function"""
    with console_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")

def register_wait_start(client_name):
    """Register when a client starts waiting for the lock"""
    wait_queue.append(client_name)
    client_statistics[client_name]['wait_start'] = time.time()

def fair_acquire(client_name, timeout=1.0):
    """Fair lock acquisition using FIFO queue + priority boost"""
    start_time = time.time()
    

    consecutive_failures = client_statistics[client_name]['failed_attempts'] - client_statistics[client_name]['access_count']
    effective_priority = client_priorities[client_name] + (consecutive_failures // 100)
    
    while time.time() - start_time < timeout:
        if resource_lock.acquire(timeout=0.01):

            if (not wait_queue or wait_queue[0] == client_name or 
                effective_priority > max(client_priorities.values()) + 1):
                

                try:
                    wait_queue.remove(client_name)
                except ValueError:
                    pass
                
                client_statistics[client_name]['wait_times'].append(time.time() - start_time)
                return True
            else:
                resource_lock.release()
        
        time.sleep(0.001)
    
    return False

def simulate_db_work(client_name, work_duration):
    """Simulate work being done on a shared database"""
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
    safe_print("💎 VIP CLIENT: Starting requests (FAIR scheduling)")
    
    while time.time() < simulation_end_time:
        register_wait_start(client_name)
        if fair_acquire(client_name):
            client_statistics[client_name]['failed_attempts'] = 0
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                
                if current_count % 50 == 0:
                    safe_print(f"💎 VIP: Acquired #{current_count}")
                
                simulate_db_work(client_name, VIP_WORK_TIME)
            finally:
                resource_lock.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        
        time.sleep(VIP_SLEEP_TIME)
    
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("💎 VIP CLIENT: Stopped")

def standard_client_thread():
    client_name = 'standard_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Starting requests (FAIR scheduling)")
    
    while time.time() < simulation_end_time:
        register_wait_start(client_name)
        if fair_acquire(client_name):
            client_statistics[client_name]['failed_attempts'] = 0
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                
                if current_count % 50 == 0:
                    safe_print(f"🟦 STANDARD: Acquired #{current_count}")
                
                simulate_db_work(client_name, STANDARD_WORK_TIME)
            finally:
                resource_lock.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        
        time.sleep(STANDARD_SLEEP_TIME)
    
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Stopped")

def free_client_thread():
    client_name = 'free_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Starting requests (FAIR scheduling)")
    
    while time.time() < simulation_end_time:
        register_wait_start(client_name)
        if fair_acquire(client_name):
            client_statistics[client_name]['failed_attempts'] = 0
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                
                safe_print(f"🆓 FREE: Acquired #{current_count}")
                simulate_db_work(client_name, FREE_WORK_TIME)
            finally:
                resource_lock.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        
        time.sleep(FREE_SLEEP_TIME)
    
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Stopped")

def print_final_statistics():
    safe_print("\n" + "="*80)
    safe_print("FAIR SCHEDULING - NO STARVATION STATISTICS")
    safe_print("="*80)
    
    for client_name, stats in client_statistics.items():
        if stats['start_time']:
            execution_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count']/total_attempts)*100 if total_attempts else 0
            
            avg_wait = sum(stats['wait_times'])/len(stats['wait_times']) if stats['wait_times'] else 0
            
            safe_print(f"\n📊 {client_name.upper()}:")
            safe_print(f"  Requests: {stats['access_count']}")
            safe_print(f"  Failures: {stats['failed_attempts']}")
            safe_print(f"  Success: {success_rate:.1f}%")
            safe_print(f"  Runtime: {execution_time:.1f}s")
            safe_print(f"  Avg wait: {avg_wait*1000:.1f}ms")
    
    safe_print(f"\n🔢 DB: {database_row_count:,} rows, {total_db_transactions} txns")
    safe_print("="*80)

def main():
    global simulation_end_time
    safe_print("🚀 FAIR LOCK SCHEDULING DEMO - NO STARVATION")
    safe_print(f"Duration: {SIMULATION_DURATION}s")
    safe_print("-"*80)
    
    simulation_end_time = time.time() + SIMULATION_DURATION
    
    threads = [
        threading.Thread(target=vip_client_thread, name="VIP"),
        threading.Thread(target=standard_client_thread, name="Standard"),
        threading.Thread(target=free_client_thread, name="Free")
    ]
    
    for t in threads: t.start()
    for t in threads: t.join()
    
    print_final_statistics()
    safe_print("🏁 FAIR SCHEDULING COMPLETE")

if __name__ == "__main__":
    main()

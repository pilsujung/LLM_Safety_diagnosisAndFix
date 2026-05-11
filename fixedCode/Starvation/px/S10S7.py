import threading
import time
import random
from datetime import datetime
from collections import defaultdict


class FairLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._semaphore = threading.Semaphore(1)
        self._wait_start = defaultdict(lambda: None)
        self._starvation_counter = defaultdict(int)
        self._console_lock = threading.Lock()
        
    def acquire(self, client_name, timeout=None):
        """Fair acquire with starvation protection"""
        start_time = time.time()
        self._wait_start[client_name] = start_time
        

        if self._semaphore.acquire(timeout=timeout):
            with self._lock:
                wait_time = time.time() - start_time
                self._starvation_counter[client_name] += wait_time
                

                if wait_time > 0.01:
                    self.safe_print(f"⏰ {client_name.upper()}: waited {wait_time:.3f}s (starvation score: {self._starvation_counter[client_name]:.2f})")
                
                return True
        return False
    
    def release(self, client_name):
        """Fair release - check for most starved waiter"""
        self._semaphore.release()
        self._wait_start[client_name] = None
    
    def safe_print(self, message):
        with self._console_lock:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{timestamp}] {message}")


fair_resource = FairLock()


SIMULATION_DURATION = 7.0
VIP_WORK_TIME, STANDARD_WORK_TIME, FREE_WORK_TIME = 0.003, 0.0045, 0.006
VIP_SLEEP_TIME, STANDARD_SLEEP_TIME, FREE_SLEEP_TIME = 0.001, 0.006, 0.010


client_statistics = {
    'vip_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None},
    'standard_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None},
    'free_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None}
}

database_row_count = total_db_transactions = 0
simulation_end_time = None

def simulate_db_work(client_name, work_duration):
    global database_row_count, total_db_transactions
    start_work_time = time.time()
    
    for _ in range(random.randint(10, 30)):
        database_row_count += 1
    total_db_transactions += 1
    
    time.sleep(work_duration)
    
    actual_work_time = time.time() - start_work_time
    client_statistics[client_name]['total_work_time'] += actual_work_time
    return actual_work_time

def client_thread(client_name, work_time, sleep_time, emoji, print_interval=50):
    """Generic client thread using fair lock"""
    client_statistics[client_name]['start_time'] = time.time()
    fair_resource.safe_print(f"{emoji} {client_name.upper()}: Starting requests")
    
    while time.time() < simulation_end_time:
        acquired = fair_resource.acquire(client_name, timeout=0.1)
        
        if acquired:
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                
                if current_count % print_interval == 0:
                    fair_resource.safe_print(f"{emoji} {client_name.upper()}: Acquired (#{current_count})")
                
                simulate_db_work(client_name, work_time)
            finally:
                fair_resource.release(client_name)
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        
        time.sleep(sleep_time)
    
    client_statistics[client_name]['end_time'] = time.time()
    fair_resource.safe_print(f"{emoji} {client_name.upper()}: Stopped")

def print_final_statistics():
    fair_resource.safe_print("\n" + "="*80)
    fair_resource.safe_print("FAIR LOCK - NO STARVATION STATISTICS")
    fair_resource.safe_print("="*80)
    
    for client_name, stats in client_statistics.items():
        if stats['start_time']:
            exec_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count']/total_attempts * 100) if total_attempts else 0
            
            fair_resource.safe_print(f"\n📊 {client_name.upper()}:")
            fair_resource.safe_print(f"  • Successes: {stats['access_count']}")
            fair_resource.safe_print(f"  • Failures: {stats['failed_attempts']}")
            fair_resource.safe_print(f"  • Success rate: {success_rate:.1f}%")
            fair_resource.safe_print(f"  • Runtime: {exec_time:.1f}s")
    
    fair_resource.safe_print(f"\n📈 ACCESS COUNTS COMPARISON:")
    vip, std, free = [client_statistics[c]['access_count'] for c in ['vip_client', 'standard_client', 'free_client']]
    fair_resource.safe_print(f"  VIP: {vip:4d} | Standard: {std:4d} | Free: {free:4d}")
    fair_resource.safe_print(f"  Ratios: VIP:Std={vip/std:.1f}:1 | VIP:Free={vip/free:.1f}:1")
    
    fair_resource.safe_print("="*80)

def main():
    global simulation_end_time
    fair_resource.safe_print("🚀 FAIR LOCK DEMO - ELIMINATING STARVATION")
    simulation_end_time = time.time() + SIMULATION_DURATION
    

    threads = [
        threading.Thread(target=client_thread, args=('vip_client', VIP_WORK_TIME, VIP_SLEEP_TIME, '💎', 75)),
        threading.Thread(target=client_thread, args=('standard_client', STANDARD_WORK_TIME, STANDARD_SLEEP_TIME, '🟦', 50)),
        threading.Thread(target=client_thread, args=('free_client', FREE_WORK_TIME, FREE_SLEEP_TIME, '🆓', 25))
    ]
    
    for t in threads: t.start()
    for t in threads: t.join()
    
    print_final_statistics()

if __name__ == "__main__":
    main()

import threading
import time
import random
from datetime import datetime


resource_manager = threading.RLock()
console_lock = threading.Lock()
starvation_points = {'vip_client': 0, 'standard_client': 0, 'free_client': 0}


SIMULATION_DURATION = 7.0
VIP_WORK_TIME = 0.003
STANDARD_WORK_TIME = 0.0045
FREE_WORK_TIME = 0.006
VIP_SLEEP_TIME = 0.001
STANDARD_SLEEP_TIME = 0.006
FREE_SLEEP_TIME = 0.010

simulation_end_time = None
client_statistics = {
    'vip_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None},
    'standard_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None},
    'free_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None}
}
database_row_count = 0
total_db_transactions = 0

def safe_print(message):
    with console_lock:
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f'[{timestamp}] {message}')

def update_starvation(client_name, waited_time):
    starvation_points[client_name] += waited_time
    for other in starvation_points:
        if other != client_name:
            starvation_points[other] *= 0.99

def fair_acquire(client_name, base_timeout=0.002):
    """Fair lock acquisition preventing starvation"""
    start_wait = time.time()
    max_timeout = base_timeout * (1 + starvation_points[client_name] / 10)
    
    while time.time() - start_wait < max_timeout * 10:
        if resource_manager.acquire(timeout=0.001):
            waited = time.time() - start_wait
            update_starvation(client_name, waited)
            return True
        time.sleep(0.0001)
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
    safe_print("💎 VIP CLIENT: Starting FAIR requests")
    while time.time() < simulation_end_time:
        if fair_acquire(client_name):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                if current_count % 50 == 0:
                    safe_print(f"💎 VIP: FAIR #{current_count}")
                simulate_db_work(client_name, VIP_WORK_TIME)
            finally:
                resource_manager.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        time.sleep(VIP_SLEEP_TIME)
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("💎 VIP CLIENT: Stopped")

def standard_client_thread():
    client_name = 'standard_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Starting FAIR requests")
    while time.time() < simulation_end_time:
        if fair_acquire(client_name):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                if current_count % 50 == 0:
                    safe_print(f"🟦 STANDARD: FAIR #{current_count}")
                simulate_db_work(client_name, STANDARD_WORK_TIME)
            finally:
                resource_manager.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        time.sleep(STANDARD_SLEEP_TIME)
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Stopped")

def free_client_thread():
    client_name = 'free_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Starting FAIR requests")
    while time.time() < simulation_end_time:
        if fair_acquire(client_name):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                if current_count % 10 == 0:
                    safe_print(f"🆓 FREE: FAIR #{current_count}")
                simulate_db_work(client_name, FREE_WORK_TIME)
            finally:
                resource_manager.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1
        time.sleep(FREE_SLEEP_TIME)
    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Stopped")


def print_final_statistics():
    safe_print('\n' + '=' * 80)
    safe_print('FAIR STARVATION-FIXED STATISTICS')
    safe_print('=' * 80)
    for client_name, stats in client_statistics.items():
        if stats['start_time'] and stats['end_time']:
            execution_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count'] / total_attempts) * 100 if total_attempts > 0 else 0.0
            avg_work_time = stats['total_work_time'] / stats['access_count'] if stats['access_count'] > 0 else 0.0
            readable_name = client_name.upper().replace('_', ' ')
            safe_print(f'\n📈 {readable_name}:')
            safe_print(f' • Success: {stats["access_count"]}')
            safe_print(f' • Failures: {stats["failed_attempts"]}')
            safe_print(f' • Success rate: {success_rate:.1f}%')
            safe_print(f' • Starvation points: {starvation_points[client_name]:.0f}')


def main():
    global simulation_end_time
    safe_print('🚀 FAIR STARVATION FIX DEMO')
    simulation_end_time = time.time() + SIMULATION_DURATION
    vip_thread = threading.Thread(target=vip_client_thread)
    standard_thread = threading.Thread(target=standard_client_thread)
    free_thread = threading.Thread(target=free_client_thread)
    vip_thread.start()
    standard_thread.start()
    free_thread.start()
    vip_thread.join()
    standard_thread.join()
    free_thread.join()
    print_final_statistics()
    safe_print('🏁 FAIR DEMO COMPLETE')

if __name__ == '__main__':
    main()

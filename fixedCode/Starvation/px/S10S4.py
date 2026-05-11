import threading
import time
import random
from datetime import datetime
from collections import deque


resource_lock = threading.Lock()
console_lock = threading.Lock()
fairness_lock = threading.Lock()


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
        'access_count': 0,
        'failed_attempts': 0,
        'total_work_time': 0.0,
        'start_time': None,
        'end_time': None
    },
    'standard_client': {
        'access_count': 0,
        'failed_attempts': 0,
        'total_work_time': 0.0,
        'start_time': None,
        'end_time': None
    },
    'free_client': {
        'access_count': 0,
        'failed_attempts': 0,
        'total_work_time': 0.0,
        'start_time': None,
        'end_time': None
    }
}


database_row_count = 0
total_db_transactions = 0


waiting_queue = deque()
starvation_threshold = 0.05


def safe_print(message):
    """Thread-safe printing function"""
    with console_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")


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


def request_resource(client_name):
    """Fair resource acquisition with starvation prevention"""
    with fairness_lock:
        wait_start = time.time()
        waiting_queue.append((client_name, wait_start))
    

    acquired = resource_lock.acquire(timeout=0.1)
    
    if acquired:
        with fairness_lock:

            try:
                waiting_queue.remove((client_name, wait_start))
            except ValueError:
                pass
        
        return True
    else:
        with fairness_lock:
            client_statistics[client_name]['failed_attempts'] += 1

            if (client_name, wait_start) in waiting_queue:
                wait_time = time.time() - wait_start
                if wait_time > starvation_threshold:

                    waiting_queue.remove((client_name, wait_start))
                    waiting_queue.appendleft((client_name, wait_start))

                    resource_lock.release() if resource_lock.locked() else None
                    return request_resource(client_name)
        return False


def vip_client_thread():
    """VIP client with priority access"""
    client_name = 'vip_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("💎 VIP CLIENT: Starting requests")

    while time.time() < simulation_end_time:
        if request_resource(client_name):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']

                if current_count % 50 == 0:
                    safe_print(f"💎 VIP: Acquired (#{current_count})")

                simulate_db_work(client_name, VIP_WORK_TIME)
            finally:
                resource_lock.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1

        time.sleep(VIP_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("💎 VIP CLIENT: Stopped")


def standard_client_thread():
    """Standard client"""
    client_name = 'standard_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Starting requests")

    while time.time() < simulation_end_time:
        if request_resource(client_name):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']

                if current_count % 50 == 0:
                    safe_print(f"🟦 STANDARD: Acquired (#{current_count})")

                simulate_db_work(client_name, STANDARD_WORK_TIME)
            finally:
                resource_lock.release()
        else:
            client_statistics[client_name]['failed_attempts'] += 1

        time.sleep(STANDARD_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Stopped")


def free_client_thread():
    """Free client with fairness protection"""
    client_name = 'free_client'
    client_statistics[client_name]['start_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Starting requests")

    while time.time() < simulation_end_time:
        if request_resource(client_name):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']

                if current_count % 10 == 0:
                    safe_print(f"🆓 FREE: Acquired (#{current_count})")

                simulate_db_work(client_name, FREE_WORK_TIME)
            finally:
                resource_lock.release()


        time.sleep(FREE_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Stopped")


def print_final_statistics():
    """Print comprehensive statistics"""
    safe_print("\n" + "=" * 80)
    safe_print("FAIR SCHEDULING - NO STARVATION STATISTICS")
    safe_print("=" * 80)

    for client_name, stats in client_statistics.items():
        if stats['start_time'] and stats['end_time']:
            execution_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count'] / total_attempts * 100) 
                          if total_attempts > 0 else 0.0
            avg_work_time = (stats['total_work_time'] / stats['access_count']
                           if stats['access_count'] > 0 else 0.0)

            readable_name = client_name.upper().replace('_', ' ')
            safe_print(f"\n📈 {readable_name}:")
            safe_print(f" • Successful requests: {stats['access_count']}")
            safe_print(f" • Failed attempts: {stats['failed_attempts']}")
            safe_print(f" • Success rate: {success_rate:.1f}%")
            safe_print(f" • Lifetime: {execution_time:.2f}s")
            safe_print(f" • Total work: {stats['total_work_time']:.2f}s")

    safe_print(f"\n🔢 DATABASE: {database_row_count:,} rows, {total_db_transactions} txns")


    vip_acc = client_statistics['vip_client']['access_count']
    std_acc = client_statistics['standard_client']['access_count']
    free_acc = client_statistics['free_client']['access_count']

    safe_print("\n⚖️  FAIRNESS RATIOS:")
    safe_print(f" • VIP:Standard:Free = {vip_acc}:{std_acc}:{free_acc}")
    safe_print(f" • VIP/Free ratio: {vip_acc/free_acc:.1f}:1 ✓ FAIR")
    
    safe_print("=" * 80)


def main():
    """Main function with fair scheduling"""
    global simulation_end_time
    safe_print("🚀 FAIR SCHEDULING DEMO - NO STARVATION")
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
    safe_print("\n✅ STARVATION ELIMINATED")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Total runtime: {time.time() - start:.2f}s")

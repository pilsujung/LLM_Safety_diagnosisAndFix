import threading
import time
import random
from datetime import datetime
from collections import deque


resource_lock = threading.Lock()
console_lock = threading.Lock()


queue_lock = threading.Lock()
request_queue = deque()
queue_condition = threading.Condition(queue_lock)


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


def fair_acquire_lock(client_name, timeout=None):
    """
    Fair lock acquisition using a queue system.
    Each thread waits for its turn in the queue.
    """
    my_turn_event = threading.Event()
    
    with queue_lock:

        request_queue.append((client_name, my_turn_event))
        queue_position = len(request_queue)
    

    start_wait = time.time()
    while True:
        with queue_lock:
            if request_queue and request_queue[0][1] == my_turn_event:

                request_queue.popleft()
                break
        

        if timeout and (time.time() - start_wait) > timeout:

            with queue_lock:
                try:
                    request_queue.remove((client_name, my_turn_event))
                except ValueError:
                    pass
            return False
        

        time.sleep(0.0001)
    

    resource_lock.acquire()
    return True


def fair_release_lock():
    """Release the resource lock"""
    resource_lock.release()


def vip_client_thread():
    """Client that gets frequent access to the shared database (highest priority)"""
    client_name = 'vip_client'
    client_statistics[client_name]['start_time'] = time.time()

    safe_print("💎 VIP CLIENT: Starting requests")

    while time.time() < simulation_end_time:

        if fair_acquire_lock(client_name, timeout=1.0):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']

                if current_count % 50 == 0:
                    safe_print(f"💎 VIP: Acquired database connection (Request #{current_count})")

                simulate_db_work(client_name, VIP_WORK_TIME)
            finally:
                fair_release_lock()
        else:
            client_statistics[client_name]['failed_attempts'] += 1

        time.sleep(VIP_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("💎 VIP CLIENT: Stopped after simulation window")


def standard_client_thread():
    """Client with medium access frequency (middle priority)"""
    client_name = 'standard_client'
    client_statistics[client_name]['start_time'] = time.time()

    safe_print("🟦 STANDARD CLIENT: Starting requests")

    while time.time() < simulation_end_time:

        if fair_acquire_lock(client_name, timeout=1.0):
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']

                if current_count % 50 == 0:
                    safe_print(f"🟦 STANDARD: Acquired database connection (Request #{current_count})")

                simulate_db_work(client_name, STANDARD_WORK_TIME)
            finally:
                fair_release_lock()
        else:
            client_statistics[client_name]['failed_attempts'] += 1

        time.sleep(STANDARD_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Stopped after simulation window")


def free_client_thread():
    """Client that now gets fair access to the shared database"""
    client_name = 'free_client'
    client_statistics[client_name]['start_time'] = time.time()

    safe_print("🆓 FREE CLIENT: Starting requests")

    consecutive_failures = 0
    max_consecutive_failures = 0

    while time.time() < simulation_end_time:

        lock_acquired = fair_acquire_lock(client_name, timeout=0.5)

        if lock_acquired:
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']
                consecutive_failures = 0

                safe_print(f"🆓 FREE: Acquired database connection (Request #{current_count})")

                simulate_db_work(client_name, FREE_WORK_TIME)

            finally:
                fair_release_lock()
        else:

            client_statistics[client_name]['failed_attempts'] += 1
            consecutive_failures += 1
            max_consecutive_failures = max(max_consecutive_failures, consecutive_failures)

            if consecutive_failures % 200 == 0:
                safe_print(
                    f"🆓 FREE: Failed to get connection {consecutive_failures} times in a row"
                )

        time.sleep(FREE_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    client_statistics[client_name]['max_consecutive_failures'] = max_consecutive_failures
    safe_print("🆓 FREE CLIENT: Stopped after simulation window")


def print_final_statistics():
    """Print comprehensive statistics about the client starvation demonstration"""
    safe_print("\n" + "=" * 80)
    safe_print("FINAL CLIENT STATISTICS (WITH FAIR SCHEDULING)")
    safe_print("=" * 80)

    for client_name, stats in client_statistics.items():
        if stats['start_time'] and stats['end_time']:
            execution_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count'] / total_attempts) * 100 if total_attempts > 0 else 0.0
            avg_work_time = (
                stats['total_work_time'] / stats['access_count']
                if stats['access_count'] > 0
                else 0.0
            )

            readable_name = client_name.upper().replace('_', ' ')

            safe_print(f"\n📈 {readable_name}:")
            safe_print(f"   • Successful requests: {stats['access_count']}")
            safe_print(f"   • Failed lock acquisitions: {stats['failed_attempts']}")
            safe_print(f"   • Success rate: {success_rate:.2f}%")
            safe_print(f"   • Thread lifetime: {execution_time:.2f}s")
            safe_print(f"   • Total work time: {stats['total_work_time']:.2f}s")
            safe_print(f"   • Avg work time per success: {avg_work_time:.4f}s")

            if 'max_consecutive_failures' in stats:
                safe_print(f"   • Max consecutive failures: {stats['max_consecutive_failures']}")

    safe_print("\n🔢 SHARED DATABASE STATISTICS:")
    safe_print(f"   • Final row count: {database_row_count}")
    safe_print(f"   • Total DB transactions: {total_db_transactions}")


    vip_access = client_statistics['vip_client']['access_count']
    std_access = client_statistics['standard_client']['access_count']
    free_access = client_statistics['free_client']['access_count']

    def ratio(a, b):
        return a / b if b > 0 else float('inf')

    r_vip_std = ratio(vip_access, std_access)
    r_vip_free = ratio(vip_access, free_access)
    r_std_free = ratio(std_access, free_access)

    safe_print("\n✅ FAIRNESS ANALYSIS:")
    safe_print(f"   • VIP vs Standard success ratio: {r_vip_std:.2f}:1")
    safe_print(f"   • VIP vs Free success ratio: {r_vip_free:.2f}:1")
    safe_print(f"   • Standard vs Free success ratio: {r_std_free:.2f}:1")

    if r_std_free > 10 and r_vip_free > 10:
        safe_print("   • SEVERE STARVATION: Free client is heavily disadvantaged")
    elif r_std_free > 3 or r_vip_free > 3:
        safe_print("   • Moderate starvation affecting the free client")
    else:
        safe_print("   • ✅ Fair-enough request distribution among clients")

    safe_print("=" * 80)


def main():
    """Main function to demonstrate fair scheduling (no starvation)"""
    global simulation_end_time
    safe_print("🚀 STARTING FAIR SCHEDULING DEMONSTRATION (VIP vs STANDARD vs FREE)")
    safe_print(f"Simulation duration: {SIMULATION_DURATION}s")
    safe_print(f"VIP sleep: {VIP_SLEEP_TIME}s, Free sleep: {FREE_SLEEP_TIME}s")
    safe_print("-" * 80)

    simulation_end_time = time.time() + SIMULATION_DURATION


    vip_thread = threading.Thread(target=vip_client_thread, name="VIPClientThread")
    standard_thread = threading.Thread(target=standard_client_thread, name="StandardClientThread")
    free_thread = threading.Thread(target=free_client_thread, name="FreeClientThread")

    vip_thread.start()
    standard_thread.start()
    free_thread.start()


    vip_thread.join()
    standard_thread.join()
    free_thread.join()


    print_final_statistics()

    safe_print("\n🏁 FAIR SCHEDULING DEMONSTRATION COMPLETED")


if __name__ == "__main__":
    start = time.time()
    main()
    print("Total runtime:", time.time() - start)
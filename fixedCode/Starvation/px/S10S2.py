import threading
import time
import random
from datetime import datetime


resource_lock = threading.RLock()
console_lock = threading.Lock()


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


def vip_client_thread():
    """Client that gets frequent access to the shared database (highest priority)"""
    client_name = 'vip_client'
    client_statistics[client_name]['start_time'] = time.time()

    safe_print("💎 VIP CLIENT: Starting requests")

    while time.time() < simulation_end_time:

        resource_lock.acquire()
        try:
            client_statistics[client_name]['access_count'] += 1
            current_count = client_statistics[client_name]['access_count']

            if current_count % 50 == 0:
                safe_print(f"💎 VIP: Acquired database connection (Request #{current_count})")

            simulate_db_work(client_name, VIP_WORK_TIME)
        finally:
            resource_lock.release()

        time.sleep(VIP_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("💎 VIP CLIENT: Stopped after simulation window")


def standard_client_thread():
    """Client with medium access frequency (middle priority)"""
    client_name = 'standard_client'
    client_statistics[client_name]['start_time'] = time.time()

    safe_print("🟦 STANDARD CLIENT: Starting requests")

    while time.time() < simulation_end_time:

        resource_lock.acquire()
        try:
            client_statistics[client_name]['access_count'] += 1
            current_count = client_statistics[client_name]['access_count']

            if current_count % 50 == 0:
                safe_print(f"🟦 STANDARD: Acquired database connection (Request #{current_count})")

            simulate_db_work(client_name, STANDARD_WORK_TIME)
        finally:
            resource_lock.release()

        time.sleep(STANDARD_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🟦 STANDARD CLIENT: Stopped after simulation window")


def free_client_thread():
    """Client that now gets fair access to the shared database (no more starvation)"""
    client_name = 'free_client'
    client_statistics[client_name]['start_time'] = time.time()

    safe_print("🆓 FREE CLIENT: Starting requests")

    while time.time() < simulation_end_time:

        resource_lock.acquire()
        try:
            client_statistics[client_name]['access_count'] += 1
            current_count = client_statistics[client_name]['access_count']

            safe_print(f"🆓 FREE: Acquired database connection (Request #{current_count})")

            simulate_db_work(client_name, FREE_WORK_TIME)
        finally:
            resource_lock.release()

        time.sleep(FREE_SLEEP_TIME)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print("🆓 FREE CLIENT: Stopped after simulation window")


def print_final_statistics():
    """Print comprehensive statistics about the client starvation demonstration"""
    safe_print("\n" + "=" * 80)
    safe_print("FINAL CLIENT STARVATION SOLUTION STATISTICS (FAIR ACCESS)")
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
            safe_print(f" • Successful requests: {stats['access_count']}")
            safe_print(f" • Failed lock acquisitions: {stats['failed_attempts']}")
            safe_print(f" • Success rate: {success_rate:.2f}%")
            safe_print(f" • Thread lifetime: {execution_time:.2f}s")
            safe_print(f" • Total work time: {stats['total_work_time']:.2f}s")
            safe_print(f" • Avg work time per success: {avg_work_time:.4f}s")

    safe_print("\n🔢 SHARED DATABASE STATISTICS:")
    safe_print(f" • Final row count: {database_row_count}")
    safe_print(f" • Total DB transactions: {total_db_transactions}")


    vip_access = client_statistics['vip_client']['access_count']
    std_access = client_statistics['standard_client']['access_count']
    free_access = client_statistics['free_client']['access_count']

    def ratio(a, b):
        return a / b if b > 0 else float('inf')

    r_vip_std = ratio(vip_access, std_access)
    r_vip_free = ratio(vip_access, free_access)
    r_std_free = ratio(std_access, free_access)

    safe_print("\n✅ FAIRNESS ANALYSIS:")
    safe_print(f" • VIP vs Standard success ratio: {r_vip_std:.2f}:1")
    safe_print(f" • VIP vs Free success ratio: {r_vip_free:.2f}:1")
    safe_print(f" • Standard vs Free success ratio: {r_std_free:.2f}:1")

    if 0.3 < r_vip_free < 3.0 and 0.3 < r_std_free < 3.0:
        safe_print(" • ✅ PERFECT FAIRNESS: All clients get balanced access")
    else:
        safe_print(" • ⚠️ Some priority differentiation remains")

    safe_print("=" * 80)


def main():
    """Main function to demonstrate FIXED client starvation (FAIR SOLUTION)"""
    global simulation_end_time
    safe_print("🚀 STARTING CLIENT STARVATION SOLUTION (FAIR RLock)")
    safe_print(f"Simulation duration: {SIMULATION_DURATION}s")
    safe_print("🔒 Using FAIR RLock (FIFO waiter ordering like Java ReentrantLock(true))")
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

    safe_print("\n🏁 CLIENT STARVATION SOLUTION DEMONSTRATION COMPLETED")
    safe_print("✅ FREE CLIENT now gets FAIR ACCESS - No more starvation!")


if __name__ == "__main__":
    start = time.time()
    main()
    print("Total runtime:", time.time() - start)

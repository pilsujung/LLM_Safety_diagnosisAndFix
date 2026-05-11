import threading
import time
import random
from datetime import datetime
from queue import Queue


resource_lock = threading.Lock()
console_lock = threading.Lock()


request_queue = Queue()


SIMULATION_DURATION = 7.0
VIP_WORK_TIME = 0.003
STANDARD_WORK_TIME = 0.0045
FREE_WORK_TIME = 0.006

VIP_SLEEP_TIME = 0.001
STANDARD_SLEEP_TIME = 0.006
FREE_SLEEP_TIME = 0.010
FREE_LOCK_TIMEOUT = 0.002


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


def client_thread(client_name, work_time, sleep_time, timeout):
    """General client thread function with work and sleeping"""
    client_statistics[client_name]['start_time'] = time.time()
    safe_print(f"🟩 {client_name.upper()}: Starting requests")

    while time.time() < simulation_end_time:

        lock_acquired = resource_lock.acquire(timeout=timeout)

        if lock_acquired:
            try:
                client_statistics[client_name]['access_count'] += 1
                current_count = client_statistics[client_name]['access_count']

                if current_count % 50 == 0:
                    safe_print(f"🟩 {client_name.upper()}: Acquired database connection (Request #{current_count})")

                simulate_db_work(client_name, work_time)

            finally:
                resource_lock.release()

        else:

            client_statistics[client_name]['failed_attempts'] += 1

        time.sleep(sleep_time)

    client_statistics[client_name]['end_time'] = time.time()
    safe_print(f"🟩 {client_name.upper()}: Stopped after simulation window")


def print_final_statistics():
    """Print comprehensive statistics about the client starvation demonstration"""
    safe_print("\n" + "=" * 80)
    safe_print("FINAL CLIENT STARVATION DEMONSTRATION STATISTICS")
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

    safe_print("\n🔢 SHARED DATABASE STATISTICS:")
    safe_print(f"   • Final row count: {database_row_count}")
    safe_print(f"   • Total DB transactions: {total_db_transactions}")


def main():
    """Main function to demonstrate client starvation"""
    global simulation_end_time
    safe_print("🚀 STARTING CLIENT STARVATION DEMONSTRATION (VIP vs STANDARD vs FREE)")
    safe_print(f"Simulation duration: {SIMULATION_DURATION}s")
    safe_print(f"VIP sleep: {VIP_SLEEP_TIME}s, Free timeout: {FREE_LOCK_TIMEOUT}s")
    safe_print("-" * 80)

    simulation_end_time = time.time() + SIMULATION_DURATION


    vip_thread = threading.Thread(target=client_thread, args=("vip_client", VIP_WORK_TIME, VIP_SLEEP_TIME, FREE_LOCK_TIMEOUT))
    standard_thread = threading.Thread(target=client_thread, args=("standard_client", STANDARD_WORK_TIME, STANDARD_SLEEP_TIME, FREE_LOCK_TIMEOUT))
    free_thread = threading.Thread(target=client_thread, args=("free_client", FREE_WORK_TIME, FREE_SLEEP_TIME, FREE_LOCK_TIMEOUT))

    vip_thread.start()
    standard_thread.start()
    free_thread.start()


    vip_thread.join()
    standard_thread.join()
    free_thread.join()


    print_final_statistics()

    safe_print("\n🏁 CLIENT STARVATION DEMONSTRATION COMPLETED")


if __name__ == "__main__":
    start = time.time()
    main()
    print("Total runtime:", time.time() - start)

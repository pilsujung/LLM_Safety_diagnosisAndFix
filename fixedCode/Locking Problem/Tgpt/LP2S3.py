import threading
import time
import random
from collections import defaultdict
import statistics


database = {'record1': {'value': 0, 'last_updated_by': None}}


record_lock = threading.Lock()


access_attempts = []
wait_times = defaultdict(list)
lock_acquisition_times = defaultdict(list)
operation_times = defaultdict(list)


LOCK_TIMEOUT = 2.0


def read_record(record_id, thread_name):
    """Read a record from the database with minimal lock holding."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))
    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")


    prework_start = time.time()
    time.sleep(random.uniform(0.01, 0.05))
    prework_time = time.time() - prework_start


    start_wait = time.time()
    acquired = record_lock.acquire(timeout=LOCK_TIMEOUT)
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)

    if not acquired:
        print(f"[{time.time():.4f}] {thread_name}: READ lock timeout after waiting {wait_time:.4f}s")
        operation_times[thread_name].append(prework_time)
        return None

    lock_acquisition_times[thread_name].append(time.time())
    try:

        operation_start = time.time()
        result = database[record_id].copy()
        cs_time = time.time() - operation_start
        total_time = prework_time + cs_time
        operation_times[thread_name].append(total_time)

        print(f"[{time.time():.4f}] {thread_name}: READ complete, "
              f"value={result['value']}, total_time={total_time:.4f}s "
              f"(prework={prework_time:.4f}s, cs={cs_time:.4f}s)")
    finally:
        record_lock.release()
        print(f"[{time.time():.4f}] {thread_name}: Released lock after READ")

    return result


def update_record(record_id, new_value, thread_name):
    """Update a record with minimal lock holding and timeout protection."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))
    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")


    prework_start = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    prework_time = time.time() - prework_start


    start_wait = time.time()
    acquired = record_lock.acquire(timeout=LOCK_TIMEOUT)
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)

    if not acquired:
        print(f"[{time.time():.4f}] {thread_name}: UPDATE lock timeout after waiting {wait_time:.4f}s")
        operation_times[thread_name].append(prework_time)
        return False

    lock_acquisition_times[thread_name].append(time.time())
    try:

        operation_start = time.time()
        current = database[record_id]['value']
        database[record_id] = {
            'value': new_value,
            'last_updated_by': thread_name
        }
        cs_time = time.time() - operation_start
        total_time = prework_time + cs_time
        operation_times[thread_name].append(total_time)

        print(f"[{time.time():.4f}] {thread_name}: UPDATE complete, "
              f"value {current} → {new_value}, total_time={total_time:.4f}s "
              f"(prework={prework_time:.4f}s, cs={cs_time:.4f}s)")
    finally:
        record_lock.release()
        print(f"[{time.time():.4f}] {thread_name}: Released lock after UPDATE")

    return True


def user_workflow(user_id):
    """Simulate a user's interaction with the database."""
    thread_name = f"User-{user_id}"


    time.sleep(random.uniform(0, 0.5))


    record = read_record('record1', thread_name)


    time.sleep(random.uniform(0.1, 0.3))


    if record:
        new_value = record['value'] + 1
        update_record('record1', new_value, thread_name)


    if random.random() > 0.5:
        time.sleep(random.uniform(0.05, 0.15))
        read_record('record1', thread_name)


def analyze_statistics():
    """Analyze and print statistics about the simulation."""
    print("\n" + "="*80)
    print("SIMULATION STATISTICS")
    print("="*80)

    sorted_attempts = sorted(access_attempts, key=lambda x: x[2])
    all_wait_times = [t for times in wait_times.values() for t in times]

    if all_wait_times:
        print(f"\nWait Time Statistics:")
        print(f"  Average: {statistics.mean(all_wait_times):.4f}s")
        print(f"  Max: {max(all_wait_times):.4f}s")
        print(f"  Min: {min(all_wait_times):.4f}s")
        if len(all_wait_times) > 1:
            print(f"  Std Dev: {statistics.stdev(all_wait_times):.4f}s")

    print("\nLock Acquisition Sequence:")
    for i, (thread, operation, _) in enumerate(sorted_attempts[:-1]):
        next_thread, next_op, _ = sorted_attempts[i + 1]
        if thread != next_thread:
            if wait_times[next_thread]:
                wait_time = wait_times[next_thread][0]
                print(f"  {thread} ({operation}) → {next_thread} ({next_op}) - Wait: {wait_time:.4f}s")
                wait_times[next_thread].pop(0)

    print("\nLock Contention Analysis:")
    high_waits = [t for t in all_wait_times if t > 0.1]
    if high_waits:
        print(f"  High waits (>0.1s): {len(high_waits)}")
        print(f"  Avg high wait: {statistics.mean(high_waits):.4f}s")

    print(f"\nFinal Database State: {database}")


def main():
    """Main entry for the simulation."""
    num_users = 10
    simulation_start = time.time()
    print(f"Starting database concurrency simulation with {num_users} users")
    print(f"Initial database state: {database}")
    print("="*80)

    threads = []
    for i in range(num_users):
        t = threading.Thread(target=user_workflow, args=(i + 1,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    simulation_time = time.time() - simulation_start
    print("\n" + "="*80)
    print(f"Simulation completed in {simulation_time:.4f} seconds")
    analyze_statistics()


if __name__ == "__main__":
    main()

import threading
import time
import random
from collections import defaultdict
import statistics


database = {'record1': {'value': 0, 'last_updated_by': None}}


read_lock = threading.Lock()
write_lock = threading.Lock()


access_attempts = []
wait_times = defaultdict(list)
lock_acquisition_times = defaultdict(list)
operation_times = defaultdict(list)

def read_record(record_id, thread_name):
    """Read a record from the database with reader-writer locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))

    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")

    start_wait = time.time()
    read_lock.acquire()
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)
    lock_acquisition_times[thread_name].append(time.time())

    print(f"[{time.time():.4f}] {thread_name}: Acquired read lock after waiting {wait_time:.4f}s")


    operation_start = time.time()
    time.sleep(random.uniform(0.01, 0.05))

    result = database[record_id].copy()

    operation_time = time.time() - operation_start
    operation_times[thread_name].append(operation_time)

    read_lock.release()
    print(f"[{time.time():.4f}] {thread_name}: READ complete, value={result['value']}, "
          f"time taken={operation_time:.4f}s")
    print(f"[{time.time():.4f}] {thread_name}: Released read lock")
    return result

def update_record(record_id, new_value, thread_name):
    """Update a record in the database with writer locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))

    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")

    start_wait = time.time()
    write_lock.acquire()
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)
    lock_acquisition_times[thread_name].append(time.time())

    print(f"[{time.time():.4f}] {thread_name}: Acquired write lock after waiting {wait_time:.4f}s")


    operation_start = time.time()
    time.sleep(random.uniform(0.05, 0.1))


    current = database[record_id]['value']
    database[record_id] = {
        'value': new_value,
        'last_updated_by': thread_name
    }

    operation_time = time.time() - operation_start
    operation_times[thread_name].append(operation_time)

    write_lock.release()
    print(f"[{time.time():.4f}] {thread_name}: UPDATE complete, value changed from {current} to {new_value}, "
          f"time taken={operation_time:.4f}s")
    print(f"[{time.time():.4f}] {thread_name}: Released write lock")
    return True

def user_workflow(user_id):
    """Simulate a user's interaction with the database."""
    thread_name = f"User-{user_id}"


    time.sleep(random.uniform(0, 0.3))


    record = read_record('record1', thread_name)


    time.sleep(random.uniform(0.05, 0.15))


    new_value = record['value'] + random.randint(1, 2)
    update_record('record1', new_value, thread_name)


    if random.random() > 0.5:
        time.sleep(random.uniform(0.02, 0.08))
        read_record('record1', thread_name)

def analyze_statistics():
    """Analyze and print statistics about the simulation."""
    print("\n" + "="*80)
    print("SIMULATION STATISTICS (READER-WRITER LOCK)")
    print("="*80)


    sorted_attempts = sorted(access_attempts, key=lambda x: x[2])
    all_wait_times = [t for times in wait_times.values() for t in times]

    if all_wait_times:
        print(f"\nWait Time Statistics:")
        print(f" Average wait time: {statistics.mean(all_wait_times):.4f}s")
        print(f" Maximum wait time: {max(all_wait_times):.4f}s")
        print(f" Minimum wait time: {min(all_wait_times):.4f}s")
        if len(all_wait_times) > 1:
            print(f" Standard deviation: {statistics.stdev(all_wait_times):.4f}s")

    print("\nLock Contention Analysis:")
    high_wait_times = [t for t in all_wait_times if t > 0.05]
    if high_wait_times:
        print(f" High wait times (>0.05s): {len(high_wait_times)}")
        print(f" Average high wait time: {statistics.mean(high_wait_times):.4f}s")

    print(f"\nFinal Database State: {database}")
    print(f" Total operations: {len(access_attempts)}")

def main():
    """Main function to run the simulation."""
    num_users = 15
    simulation_start = time.time()

    print(f"Starting database concurrency simulation with {num_users} users (READER-WRITER LOCK)")
    print(f"Initial database state: {database}")
    print("="*80)

    threads = [threading.Thread(target=user_workflow, args=(i+1,)) for i in range(num_users)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    simulation_time = time.time() - simulation_start
    print("\n" + "="*80)
    print(f"Simulation completed in {simulation_time:.4f} seconds")
    analyze_statistics()

if __name__ == "__main__":
    main()

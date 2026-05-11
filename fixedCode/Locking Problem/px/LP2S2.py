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

def read_record(record_id, thread_name):
    """Read a record from the database with non-blocking locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))

    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")

    start_wait = time.time()
    acquired = record_lock.acquire(timeout=1.0)
    wait_time = time.time() - start_wait
    
    if acquired:
        try:
            wait_times[thread_name].append(wait_time)
            lock_acquisition_times[thread_name].append(time.time())

            print(f"[{time.time():.4f}] {thread_name}: Acquired lock for READ after waiting {wait_time:.4f}s")


            operation_start = time.time()
            time.sleep(random.uniform(0.01, 0.05))

            result = database[record_id].copy()

            operation_time = time.time() - operation_start
            operation_times[thread_name].append(operation_time)

            print(f"[{time.time():.4f}] {thread_name}: READ complete, value={result['value']}, "
                  f"time taken={operation_time:.4f}s")

            print(f"[{time.time():.4f}] {thread_name}: Released lock after READ")
            return result
        finally:
            record_lock.release()
    else:
        print(f"[{time.time():.4f}] {thread_name}: Could not acquire lock for READ after {wait_time:.4f}s, moving on")
        return None

def update_record(record_id, new_value, thread_name):
    """Update a record in the database with non-blocking locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))

    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")

    start_wait = time.time()
    acquired = record_lock.acquire(timeout=2.0)
    wait_time = time.time() - start_wait
    
    if acquired:
        try:
            wait_times[thread_name].append(wait_time)
            lock_acquisition_times[thread_name].append(time.time())

            print(f"[{time.time():.4f}] {thread_name}: Acquired lock for UPDATE after waiting {wait_time:.4f}s")


            operation_start = time.time()
            time.sleep(random.uniform(0.05, 0.2))


            current = database[record_id]['value']
            database[record_id] = {
                'value': new_value,
                'last_updated_by': thread_name
            }

            operation_time = time.time() - operation_start
            operation_times[thread_name].append(operation_time)

            print(f"[{time.time():.4f}] {thread_name}: UPDATE complete, value changed from {current} to {new_value}, "
                  f"time taken={operation_time:.4f}s")

            print(f"[{time.time():.4f}] {thread_name}: Released lock after UPDATE")
            return True
        finally:
            record_lock.release()
    else:
        print(f"[{time.time():.4f}] {thread_name}: Could not acquire lock for UPDATE after {wait_time:.4f}s, moving on")
        return False

def user_workflow(user_id):
    """Simulate a user's interaction with the database."""
    thread_name = f"User-{user_id}"


    time.sleep(random.uniform(0, 0.5))


    record = read_record('record1', thread_name)
    

    if record is None:
        print(f"[{time.time():.4f}] {thread_name}: Skipping update due to read timeout")
        return


    time.sleep(random.uniform(0.1, 0.3))


    new_value = record['value'] + 1
    update_success = update_record('record1', new_value, thread_name)
    

    if random.random() > 0.5 and update_success:
        time.sleep(random.uniform(0.05, 0.15))
        read_record('record1', thread_name)

def analyze_statistics():
    """Analyze and print statistics about the simulation."""
    print("\n" + "="*80)
    print("SIMULATION STATISTICS (NON-BLOCKING)")
    print("="*80)


    sorted_attempts = sorted(access_attempts, key=lambda x: x[2])


    all_wait_times = [time for user_times in wait_times.values() for time in user_times]

    if all_wait_times:
        print(f"\nWait Time Statistics:")
        print(f" Average wait time: {statistics.mean(all_wait_times):.4f}s")
        print(f" Maximum wait time: {max(all_wait_times):.4f}s")
        print(f" Minimum wait time: {min(all_wait_times):.4f}s")
        if len(all_wait_times) > 1:
            print(f" Standard deviation: {statistics.stdev(all_wait_times):.4f}s")


    total_attempts = len(access_attempts)
    successful_acquires = len(all_wait_times)
    timeout_count = total_attempts - successful_acquires
    print(f"\nLock Acquisition Summary:")
    print(f" Total attempts: {total_attempts}")
    print(f" Successful acquires: {successful_acquires}")
    print(f" Timeouts: {timeout_count} ({timeout_count/total_attempts*100:.1f}%)")


    print(f"\nFinal Database State: {database}")

def main():
    """Main function to run the simulation."""
    num_users = 10
    simulation_start = time.time()

    print(f"Starting NON-BLOCKING database concurrency simulation with {num_users} users")
    print(f"Initial database state: {database}")
    print("="*80)


    threads = []
    for i in range(num_users):
        thread = threading.Thread(target=user_workflow, args=(i+1,))
        threads.append(thread)
        thread.start()


    for thread in threads:
        thread.join()

    simulation_time = time.time() - simulation_start
    print("\n" + "="*80)
    print(f"Simulation completed in {simulation_time:.4f} seconds")


    analyze_statistics()

if __name__ == "__main__":
    main()

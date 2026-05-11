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
    """Read a record from the database with proper locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))
    
    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")
    
    start_wait = time.time()
    with record_lock:
        wait_time = time.time() - start_wait
        wait_times[thread_name].append(wait_time)
        lock_acquisition_times[thread_name].append(time.time())
        
        print(f"[{time.time():.4f}] {thread_name}: Acquired lock for READ after waiting {wait_time:.4f}s")
        

        operation_start = time.time()
        time.sleep(random.uniform(0.01, 0.05))
        
        result = database[record_id].copy()
        
        operation_time = time.time() - operation_start
        operation_times[thread_name].append(operation_time)
        
        print(
            f"[{time.time():.4f}] {thread_name}: READ complete, "
            f"value={result['value']}, time taken={operation_time:.4f}s"
        )
    
    print(f"[{time.time():.4f}] {thread_name}: Released lock after READ")
    return result


def update_record(record_id, new_value, thread_name):
    """
    Update a record in the database with minimized blocking on the lock.

    Fix: Do the long 'work' OUTSIDE the lock and only hold the lock
    for the short critical section that actually touches shared state.
    """
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))
    
    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")
    

    operation_start = time.time()
    simulated_work_time = random.uniform(0.05, 0.2)
    time.sleep(simulated_work_time)
    

    start_wait = time.time()
    with record_lock:
        wait_time = time.time() - start_wait
        wait_times[thread_name].append(wait_time)
        lock_acquisition_times[thread_name].append(time.time())
        
        print(
            f"[{time.time():.4f}] {thread_name}: "
            f"Acquired lock for UPDATE after waiting {wait_time:.4f}s"
        )
        

        current = database[record_id]['value']
        database[record_id] = {
            'value': new_value,
            'last_updated_by': thread_name
        }
        

        operation_time = time.time() - operation_start
        operation_times[thread_name].append(operation_time)
        
        print(
            f"[{time.time():.4f}] {thread_name}: UPDATE complete, "
            f"value changed from {current} to {new_value}, "
            f"time taken={operation_time:.4f}s"
        )
    
    print(f"[{time.time():.4f}] {thread_name}: Released lock after UPDATE")
    return True


def user_workflow(user_id):
    """Simulate a user's interaction with the database."""
    thread_name = f"User-{user_id}"
    

    time.sleep(random.uniform(0, 0.5))
    

    record = read_record('record1', thread_name)
    

    time.sleep(random.uniform(0.1, 0.3))
    

    new_value = record['value'] + 1
    update_record('record1', new_value, thread_name)
    

    if random.random() > 0.5:
        time.sleep(random.uniform(0.05, 0.15))
        read_record('record1', thread_name)


def analyze_statistics():
    """Analyze and print statistics about the simulation."""
    print("\n" + "=" * 80)
    print("SIMULATION STATISTICS")
    print("=" * 80)
    

    sorted_attempts = sorted(access_attempts, key=lambda x: x[2])
    

    all_wait_times = [t for user_times in wait_times.values() for t in user_times]
    
    if all_wait_times:
        print(f"\nWait Time Statistics:")
        print(f"  Average wait time: {statistics.mean(all_wait_times):.4f}s")
        print(f"  Maximum wait time: {max(all_wait_times):.4f}s")
        print(f"  Minimum wait time: {min(all_wait_times):.4f}s")
        if len(all_wait_times) > 1:
            print(f"  Standard deviation: {statistics.stdev(all_wait_times):.4f}s")
    

    print("\nLock Acquisition Sequence:")
    for i, (thread, operation, _) in enumerate(sorted_attempts):
        next_idx = i + 1
        if next_idx < len(sorted_attempts):
            next_thread, next_op, next_time = sorted_attempts[next_idx]
            if thread != next_thread:
                if lock_acquisition_times[next_thread] and wait_times[next_thread]:
                    acquisition_time = lock_acquisition_times[next_thread][0]
                    wait_time = wait_times[next_thread][0]
                    print(
                        f"  {thread} ({operation}) → "
                        f"{next_thread} ({next_op}) - Wait: {wait_time:.4f}s"
                    )
                    

                    lock_acquisition_times[next_thread].pop(0)
                    wait_times[next_thread].pop(0)
    

    print("\nLock Contention Analysis:")
    high_wait_times = [t for t in all_wait_times if t > 0.1]
    if high_wait_times:
        print(f"  Number of high wait times (>0.1s): {len(high_wait_times)}")
        print(f"  Average high wait time: {statistics.mean(high_wait_times):.4f}s")
        

        contention_points = []
        for i in range(len(sorted_attempts)):
            current_time = sorted_attempts[i][2]

            waiting_threads = sum(
                1
                for j in range(len(sorted_attempts))
                if i != j and abs(sorted_attempts[j][2] - current_time) < 0.1
            )
            if waiting_threads > 0:
                contention_points.append((current_time, waiting_threads))
        
        if contention_points:
            max_contention = max(contention_points, key=lambda x: x[1])
            print(
                f"  Peak contention: {max_contention[1]} threads "
                f"at time {max_contention[0]:.4f}s"
            )
    

    print(f"\nFinal Database State: {database}")


def main():
    """Main function to run the simulation."""
    num_users = 10
    simulation_start = time.time()
    
    print(f"Starting database concurrency simulation with {num_users} users")
    print(f"Initial database state: {database}")
    print("=" * 80)
    

    threads = []
    for i in range(num_users):
        thread = threading.Thread(target=user_workflow, args=(i + 1,))
        threads.append(thread)
        thread.start()
    

    for thread in threads:
        thread.join()
    
    simulation_time = time.time() - simulation_start
    print("\n" + "=" * 80)
    print(f"Simulation completed in {simulation_time:.4f} seconds")
    

    analyze_statistics()


if __name__ == "__main__":
    main()

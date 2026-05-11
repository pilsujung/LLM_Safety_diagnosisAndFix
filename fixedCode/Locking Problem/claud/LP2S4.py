import threading
import time
import random
from collections import defaultdict
import statistics


database = {'record1': {'value': 0, 'last_updated_by': None}}


class ReadWriteLock:
    """A lock that allows multiple readers or one writer."""
    def __init__(self):
        self._readers = 0
        self._writers = 0
        self._read_ready = threading.Condition(threading.Lock())
        self._write_ready = threading.Condition(threading.Lock())
    
    def acquire_read(self):
        """Acquire a read lock. Blocks only if a writer is active."""
        self._read_ready.acquire()
        try:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._read_ready.release()
    
    def release_read(self):
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()
    
    def acquire_write(self):
        """Acquire a write lock. Blocks if any readers or writers are active."""
        self._write_ready.acquire()
        try:
            while self._writers > 0:
                self._write_ready.wait()
            self._writers += 1
        finally:
            self._write_ready.release()
        
        self._read_ready.acquire()
        try:
            while self._readers > 0:
                self._read_ready.wait()
        finally:
            self._read_ready.release()
    
    def release_write(self):
        """Release a write lock."""
        self._write_ready.acquire()
        try:
            self._writers -= 1
            self._write_ready.notifyAll()
            self._read_ready.notifyAll()
        finally:
            self._write_ready.release()


rw_lock = ReadWriteLock()


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
    rw_lock.acquire_read()
    try:
        wait_time = time.time() - start_wait
        wait_times[thread_name].append(wait_time)
        lock_acquisition_times[thread_name].append(time.time())
        
        print(f"[{time.time():.4f}] {thread_name}: Acquired READ lock after waiting {wait_time:.4f}s")
        

        operation_start = time.time()
        time.sleep(random.uniform(0.01, 0.05))
        
        result = database[record_id].copy()
        
        operation_time = time.time() - operation_start
        operation_times[thread_name].append(operation_time)
        
        print(f"[{time.time():.4f}] {thread_name}: READ complete, value={result['value']}, " 
              f"time taken={operation_time:.4f}s")
    finally:
        rw_lock.release_read()
        print(f"[{time.time():.4f}] {thread_name}: Released READ lock")
    
    return result

def update_record(record_id, new_value, thread_name):
    """Update a record in the database with proper locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))
    
    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")
    
    start_wait = time.time()
    rw_lock.acquire_write()
    try:
        wait_time = time.time() - start_wait
        wait_times[thread_name].append(wait_time)
        lock_acquisition_times[thread_name].append(time.time())
        
        print(f"[{time.time():.4f}] {thread_name}: Acquired WRITE lock after waiting {wait_time:.4f}s")
        

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
    finally:
        rw_lock.release_write()
        print(f"[{time.time():.4f}] {thread_name}: Released WRITE lock")
    
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
    print("\n" + "="*80)
    print("SIMULATION STATISTICS")
    print("="*80)
    

    sorted_attempts = sorted(access_attempts, key=lambda x: x[2])
    

    all_wait_times = [time for user_times in wait_times.values() for time in user_times]
    
    if all_wait_times:
        print(f"\nWait Time Statistics:")
        print(f"  Average wait time: {statistics.mean(all_wait_times):.4f}s")
        print(f"  Maximum wait time: {max(all_wait_times):.4f}s")
        print(f"  Minimum wait time: {min(all_wait_times):.4f}s")
        if len(all_wait_times) > 1:
            print(f"  Standard deviation: {statistics.stdev(all_wait_times):.4f}s")
    

    print("\nConcurrency Analysis:")
    read_count = sum(1 for _, op, _ in access_attempts if op == 'read')
    write_count = sum(1 for _, op, _ in access_attempts if op == 'write')
    print(f"  Total read operations: {read_count}")
    print(f"  Total write operations: {write_count}")
    

    print("\nLock Contention Analysis:")
    high_wait_times = [t for t in all_wait_times if t > 0.1]
    if high_wait_times:
        print(f"  Number of high wait times (>0.1s): {len(high_wait_times)}")
        print(f"  Average high wait time: {statistics.mean(high_wait_times):.4f}s")
    else:
        print(f"  No significant wait times detected (all <0.1s)")
    

    print(f"\nFinal Database State: {database}")

def main():
    """Main function to run the simulation."""
    num_users = 10
    simulation_start = time.time()
    
    print(f"Starting database concurrency simulation with {num_users} users")
    print(f"Initial database state: {database}")
    print("Using Reader-Writer Lock for better concurrency")
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
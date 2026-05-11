import threading
import time
import random
from collections import defaultdict
import statistics


database = {'record1': {'value': 0, 'last_updated_by': None}}


class ReadersWriterLock:
    """
    Readers-Writer Lock implementation that allows:
    - Multiple readers simultaneously
    - Exclusive access for writers
    - Writers have priority to prevent starvation
    """
    def __init__(self):
        self.readers = 0
        self.writers = 0
        self.read_ready = threading.Condition(threading.Lock())
        self.write_ready = threading.Condition(threading.Lock())
    
    def acquire_read(self):
        """Acquire read lock - multiple readers allowed"""
        self.read_ready.acquire()

        while self.writers > 0:
            self.read_ready.wait()
        self.readers += 1
        self.read_ready.release()
    
    def release_read(self):
        """Release read lock"""
        self.read_ready.acquire()
        self.readers -= 1

        if self.readers == 0:
            self.read_ready.notify_all()
        self.read_ready.release()
    
    def acquire_write(self):
        """Acquire write lock - exclusive access"""
        self.write_ready.acquire()
        self.writers += 1
        self.write_ready.release()
        
        self.read_ready.acquire()

        while self.readers > 0:
            self.read_ready.wait()
        self.read_ready.release()
    
    def release_write(self):
        """Release write lock"""
        self.write_ready.acquire()
        self.writers -= 1
        self.write_ready.release()
        

        self.read_ready.acquire()
        self.read_ready.notify_all()
        self.read_ready.release()


record_lock = ReadersWriterLock()


access_attempts = []
wait_times = defaultdict(list)
lock_acquisition_times = defaultdict(list)
operation_times = defaultdict(list)
concurrent_readers = []

def read_record(record_id, thread_name):
    """Read a record from the database with proper locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))
    
    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")
    

    start_wait = time.time()
    record_lock.acquire_read()
    
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)
    lock_acquisition_times[thread_name].append(time.time())
    

    current_readers = record_lock.readers
    concurrent_readers.append(current_readers)
    
    print(f"[{time.time():.4f}] {thread_name}: Acquired READ lock after waiting {wait_time:.4f}s "
          f"(concurrent readers: {current_readers})")
    
    try:

        operation_start = time.time()
        time.sleep(random.uniform(0.01, 0.05))
        
        result = database[record_id].copy()
        
        operation_time = time.time() - operation_start
        operation_times[thread_name].append(operation_time)
        
        print(f"[{time.time():.4f}] {thread_name}: READ complete, value={result['value']}, " 
              f"time taken={operation_time:.4f}s")
        
        return result
    finally:

        record_lock.release_read()
        print(f"[{time.time():.4f}] {thread_name}: Released READ lock")

def update_record(record_id, new_value, thread_name):
    """Update a record in the database with proper locking."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))
    
    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")
    

    start_wait = time.time()
    record_lock.acquire_write()
    
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)
    lock_acquisition_times[thread_name].append(time.time())
    
    print(f"[{time.time():.4f}] {thread_name}: Acquired WRITE lock (exclusive) after waiting {wait_time:.4f}s")
    
    try:

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
        
        return True
    finally:

        record_lock.release_write()
        print(f"[{time.time():.4f}] {thread_name}: Released WRITE lock")

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
    

    if concurrent_readers:
        print(f"\nConcurrent Reader Statistics:")
        print(f"  Average concurrent readers: {statistics.mean(concurrent_readers):.2f}")
        print(f"  Maximum concurrent readers: {max(concurrent_readers)}")
        print(f"  Times with multiple readers: {sum(1 for r in concurrent_readers if r > 1)}")
    

    read_ops = [op for _, op, _ in sorted_attempts if op == 'read']
    write_ops = [op for _, op, _ in sorted_attempts if op == 'write']
    print(f"\nOperation Counts:")
    print(f"  Total reads: {len(read_ops)}")
    print(f"  Total writes: {len(write_ops)}")
    

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
    print(f"Using Readers-Writer Lock for improved concurrency")
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
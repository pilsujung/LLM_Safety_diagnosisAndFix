import threading
import time
import random
from collections import defaultdict
import statistics




class RWLock:
    """Writer-preferring reader-writer lock to avoid writer starvation."""
    def __init__(self):
        self._lock = threading.Lock()
        self._read_ready = threading.Condition(self._lock)
        self._readers = 0
        self._writer_active = False
        self._writers_waiting = 0

    def acquire_read(self, timeout=None):
        start = time.time()
        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                remaining = None if timeout is None else timeout - (time.time() - start)
                if timeout is not None and remaining <= 0:
                    return False
                self._read_ready.wait(timeout=remaining)
            self._readers += 1
            return True

    def release_read(self):
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self, timeout=None):
        start = time.time()
        with self._read_ready:
            self._writers_waiting += 1
            try:
                while self._writer_active or self._readers > 0:
                    remaining = None if timeout is None else timeout - (time.time() - start)
                    if timeout is not None and remaining <= 0:
                        self._writers_waiting -= 1
                        return False
                    self._read_ready.wait(timeout=remaining)
                self._writers_waiting -= 1
                self._writer_active = True
                return True
            except:

                self._writers_waiting = max(0, self._writers_waiting - 1)
                raise

    def release_write(self):
        with self._read_ready:
            self._writer_active = False

            self._read_ready.notify_all()




database = {'record1': {'value': 0, 'last_updated_by': None}}


rw_lock = RWLock()


access_attempts = []
wait_times = defaultdict(list)
lock_acquisition_times = defaultdict(list)
operation_times = defaultdict(list)

def read_record(record_id, thread_name):
    """Read a record from the database with minimal critical section and RW lock."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))
    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")

    start_wait = time.time()
    acquired = rw_lock.acquire_read()
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)
    lock_acquisition_times[thread_name].append(time.time())
    if not acquired:
        print(f"[{time.time():.4f}] {thread_name}: FAILED to acquire READ lock")
        return None

    try:
        print(f"[{time.time():.4f}] {thread_name}: Acquired READ after waiting {wait_time:.4f}s")

        snapshot = database[record_id].copy()
    finally:
        rw_lock.release_read()
        print(f"[{time.time():.4f}] {thread_name}: Released lock after READ")


    operation_start = time.time()
    time.sleep(random.uniform(0.01, 0.05))
    operation_time = time.time() - operation_start
    operation_times[thread_name].append(operation_time)
    print(f"[{time.time():.4f}] {thread_name}: READ complete, value={snapshot['value']}, time taken={operation_time:.4f}s")
    return snapshot

def update_record(record_id, new_value, thread_name):
    """Update a record with minimal critical section and writer priority."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))
    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")


    prework_start = time.time()
    time.sleep(random.uniform(0.02, 0.06))
    prework_time = time.time() - prework_start

    start_wait = time.time()
    acquired = rw_lock.acquire_write()
    wait_time = time.time() - start_wait
    wait_times[thread_name].append(wait_time)
    lock_acquisition_times[thread_name].append(time.time())
    if not acquired:
        print(f"[{time.time():.4f}] {thread_name}: FAILED to acquire WRITE lock")
        return False

    try:
        print(f"[{time.time():.4f}] {thread_name}: Acquired UPDATE after waiting {wait_time:.4f}s")

        current = database[record_id]['value']
        database[record_id] = {'value': new_value, 'last_updated_by': thread_name}
    finally:
        rw_lock.release_write()
        print(f"[{time.time():.4f}] {thread_name}: Released lock after UPDATE")


    operation_start = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    operation_time = time.time() - operation_start
    operation_times[thread_name].append(operation_time)

    print(f"[{time.time():.4f}] {thread_name}: UPDATE complete, value changed from {current} to {new_value}, "
          f"prep={prework_time:.4f}s, post-write={operation_time:.4f}s")
    return True

def user_workflow(user_id):
    thread_name = f"User-{user_id}"
    time.sleep(random.uniform(0, 0.5))

    record = read_record('record1', thread_name)
    time.sleep(random.uniform(0.1, 0.3))
    new_value = (record['value'] if record else 0) + 1
    update_record('record1', new_value, thread_name)

    if random.random() > 0.5:
        time.sleep(random.uniform(0.05, 0.15))
        read_record('record1', thread_name)

def analyze_statistics():
    print("\n" + "="*80)
    print("SIMULATION STATISTICS")
    print("="*80)

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

    tmp_wait = {k: v.copy() for k, v in wait_times.items()}
    for i, (thread, operation, _) in enumerate(sorted_attempts[:-1]):
        next_thread, next_op, _ = sorted_attempts[i + 1]
        if thread != next_thread and tmp_wait.get(next_thread):
            w = tmp_wait[next_thread].pop(0)
            print(f"  {thread} ({operation}) → {next_thread} ({next_op}) - Wait: {w:.4f}s")

    print("\nLock Contention Analysis:")
    high_wait_times = [t for t in all_wait_times if t > 0.1]
    if high_wait_times:
        print(f"  Number of high wait times (>0.1s): {len(high_wait_times)}")
        print(f"  Average high wait time: {statistics.mean(high_wait_times):.4f}s")

    print(f"\nFinal Database State: {database}")

def main():
    num_users = 10
    simulation_start = time.time()

    print(f"Starting database concurrency simulation with {num_users} users")
    print(f"Initial database state: {database}")
    print("="*80)

    threads = [threading.Thread(target=user_workflow, args=(i + 1,)) for i in range(num_users)]
    for t in threads: t.start()
    for t in threads: t.join()

    simulation_time = time.time() - simulation_start
    print("\n" + "="*80)
    print(f"Simulation completed in {simulation_time:.4f} seconds")

    analyze_statistics()

if __name__ == "__main__":
    main()

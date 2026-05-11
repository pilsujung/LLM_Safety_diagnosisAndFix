import threading
import time
import random
from collections import defaultdict
import statistics


database = {'record1': {'value': 0, 'last_updated_by': None}}


class RWLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._ok_to_read = threading.Condition(self._lock)
        self._ok_to_write = threading.Condition(self._lock)
        self._active_readers = 0
        self._active_writers = 0
        self._waiting_writers = 0

    def acquire_read(self):
        with self._lock:
            while self._active_writers > 0 or self._waiting_writers > 0:
                self._ok_to_read.wait()
            self._active_readers += 1

    def release_read(self):
        with self._lock:
            self._active_readers -= 1
            if self._active_readers == 0:
                self._ok_to_write.notify()

    def acquire_write(self):
        with self._lock:
            self._waiting_writers += 1
            while self._active_writers > 0 or self._active_readers > 0:
                self._ok_to_write.wait()
            self._waiting_writers -= 1
            self._active_writers = 1

    def release_write(self):
        with self._lock:
            self._active_writers = 0

            if self._waiting_writers > 0:
                self._ok_to_write.notify()
            else:
                self._ok_to_read.notify_all()

rwlock = RWLock()


access_attempts = []
wait_times = defaultdict(list)
lock_acquisition_times = defaultdict(list)
operation_times = defaultdict(list)

def read_record(record_id, thread_name):
    """Read a record with a writer-preferring RW lock. No sleeping in CS."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'read', attempt_time))

    print(f"[{time.time():.4f}] {thread_name}: Attempting to READ record {record_id}")


    start_wait = time.time()
    rwlock.acquire_read()
    try:
        waited = time.time() - start_wait
        wait_times[thread_name].append(waited)
        lock_acquisition_times[thread_name].append(time.time())
        print(f"[{time.time():.4f}] {thread_name}: Acquired READ after {waited:.4f}s")


        snapshot = database[record_id].copy()
    finally:
        rwlock.release_read()
        print(f"[{time.time():.4f}] {thread_name}: Released READ lock")


    op_start = time.time()
    time.sleep(random.uniform(0.01, 0.05))
    op_time = time.time() - op_start
    operation_times[thread_name].append(op_time)

    print(f"[{time.time():.4f}] {thread_name}: READ done, value={snapshot['value']}, time={op_time:.4f}s")
    return snapshot

def update_record(record_id, new_value, thread_name):
    """Write with writer-preferring RW lock. No sleeping in CS."""
    attempt_time = time.time()
    access_attempts.append((thread_name, 'write', attempt_time))

    print(f"[{time.time():.4f}] {thread_name}: Attempting to UPDATE record {record_id}")


    prepared = {'value': new_value, 'last_updated_by': thread_name}


    start_wait = time.time()
    rwlock.acquire_write()
    try:
        waited = time.time() - start_wait
        wait_times[thread_name].append(waited)
        lock_acquisition_times[thread_name].append(time.time())
        print(f"[{time.time():.4f}] {thread_name}: Acquired WRITE after {waited:.4f}s")


        current = database[record_id]['value']
        database[record_id] = prepared
    finally:
        rwlock.release_write()
        print(f"[{time.time():.4f}] {thread_name}: Released WRITE lock")


    op_start = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    op_time = time.time() - op_start
    operation_times[thread_name].append(op_time)

    print(f"[{time.time():.4f}] {thread_name}: UPDATE complete, {current} → {new_value}, time={op_time:.4f}s")
    return True

def user_workflow(user_id):
    thread_name = f"User-{user_id}"
    time.sleep(random.uniform(0, 0.5))

    rec = read_record('record1', thread_name)
    time.sleep(random.uniform(0.1, 0.3))

    new_value = rec['value'] + 1
    update_record('record1', new_value, thread_name)

    if random.random() > 0.5:
        time.sleep(random.uniform(0.05, 0.15))
        read_record('record1', thread_name)

def analyze_statistics():
    print("\n" + "="*80)
    print("SIMULATION STATISTICS")
    print("="*80)

    sorted_attempts = sorted(access_attempts, key=lambda x: x[2])
    all_wait = [t for lst in wait_times.values() for t in lst]
    if all_wait:
        print(f"\nWait Time Statistics:")
        print(f"  Average wait: {statistics.mean(all_wait):.4f}s")
        print(f"  Max wait:     {max(all_wait):.4f}s")
        print(f"  Min wait:     {min(all_wait):.4f}s")
        if len(all_wait) > 1:
            print(f"  Std dev:      {statistics.stdev(all_wait):.4f}s")

    print("\nLock Acquisition Sequence:")

    lat = {k: v[:] for k, v in lock_acquisition_times.items()}
    wt = {k: v[:] for k, v in wait_times.items()}
    for i, (thread, op, _) in enumerate(sorted_attempts):
        nxt = i + 1
        if nxt < len(sorted_attempts):
            next_thread, next_op, _ = sorted_attempts[nxt]
            if thread != next_thread and wt.get(next_thread):
                print(f"  {thread} ({op}) → {next_thread} ({next_op}) - Wait: {wt[next_thread][0]:.4f}s")
                if lat.get(next_thread): lat[next_thread].pop(0)
                wt[next_thread].pop(0)

    print("\nLock Contention Analysis:")
    high = [t for t in all_wait if t > 0.1]
    if high:
        print(f"  High wait count (>0.1s): {len(high)}")
        print(f"  Avg high wait:          {statistics.mean(high):.4f}s")

    print(f"\nFinal Database State: {database}")

def main():
    num_users = 10
    sim_start = time.time()

    print(f"Starting database concurrency simulation with {num_users} users")
    print(f"Initial database state: {database}")
    print("="*80)

    threads = []
    for i in range(num_users):
        t = threading.Thread(target=user_workflow, args=(i+1,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    sim_time = time.time() - sim_start
    print("\n" + "="*80)
    print(f"Simulation completed in {sim_time:.4f} seconds")
    analyze_statistics()

if __name__ == "__main__":
    main()

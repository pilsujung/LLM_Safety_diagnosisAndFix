import multiprocessing as mp
import time
import random
from enum import Enum

class Priority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3

    def __str__(self) -> str:
        return self.name

class ResourceManager:
    """
    Hierarchical lock-based resource manager applying the Lock Cohorting (2012) mechanism.
    Prevents starvation while maintaining fairness between virtual NUMA node groups.
    """
    def __init__(self, manager, threshold=10, starvation_threshold=10.0):
        # Lock for protecting shared state
        self.state_lock = manager.Lock()
        
        # Global lock (G) and per-node local locks (S_i)
        self.global_lock = manager.Lock()
        self.local_locks = {
            0: manager.Lock(), 
            1: manager.Lock()
        }
        
        # Cohorting state variables
        self.global_owner = manager.Value('i', -1)
        self.handoff_count = manager.Value('i', 0)
        self.local_waiters = manager.dict({0: 0, 1: 0})
        
        # Set control thresholds
        self.threshold = threshold
        self.starvation_threshold = starvation_threshold

        # Proxy objects for recording statistics data
        self.access_logs = manager.list()
        self.wait_times = manager.list()
        self.starvation_events = manager.list()

    def request_access(self, process_id, priority, node_id):
        """Resource access request (Lock Cohorting Acquire)"""
        req_time = time.time()
        
        log_entry = f"[{req_time % 100:.2f}] + Process #{process_id} (Node {node_id}, {priority}) requested"
        self.access_logs.append(log_entry)
        print(log_entry)

        # 1. Increase the local waiter count
        with self.state_lock:
            self.local_waiters[node_id] += 1
            
        # 2. Acquire the local lock (S_i)
        self.local_locks[node_id].acquire()
        
        # 3. Decrease the local waiter count and check global lock ownership
        with self.state_lock:
            self.local_waiters[node_id] -= 1
            current_owner = self.global_owner.value
            
        # 4. If the global lock is owned by another node, acquire the global lock (G)
        if current_owner != node_id:
            self.global_lock.acquire()
            with self.state_lock:
                self.global_owner.value = node_id
                self.handoff_count.value = 0

        # === Enter Critical Section ===
        wait_time = time.time() - req_time
        
        # Check for starvation (whether the starvation threshold was exceeded)
        if wait_time > self.starvation_threshold:
            self.starvation_events.append((process_id, priority.name, wait_time))
            
        self.wait_times.append((process_id, priority.name, wait_time))
        
        log_entry = f"[{time.time() % 100:.2f}] ▶ Start Process #{process_id} (Node {node_id}, {priority}) wait={wait_time:.2f}"
        self.access_logs.append(log_entry)
        print(log_entry)

    def release_access(self, process_id, priority, node_id, duration):
        """Resource access release (Lock Cohorting Release)"""
        log_entry = f"[{time.time() % 100:.2f}] ✓ Done Process #{process_id} (Node {node_id}, {priority}) dur={duration:.2f}"
        self.access_logs.append(log_entry)
        print(log_entry)

        # 5. Evaluate Cohorting release
        with self.state_lock:
            waiters = self.local_waiters[node_id]
            handoffs = self.handoff_count.value

        # If there are waiters and the consecutive handoff threshold has not been reached -> Local Handoff
        if waiters > 0 and handoffs < self.threshold:
            with self.state_lock:
                self.handoff_count.value = handoffs + 1
            # Keep the global lock and release only the local lock (handoff to a waiter on the same node)
            self.local_locks[node_id].release()
        
        # If there are no waiters or the threshold is exceeded -> Global Release (may-pass-local)
        else:
            with self.state_lock:
                self.global_owner.value = -1
                self.handoff_count.value = 0
            self.global_lock.release()
            self.local_locks[node_id].release()

def worker_process(process_id, node_id, priority, num_requests, rm, delay_range):
    """
    Worker process that accesses the resource using Lock Cohorting on the assigned virtual NUMA node
    """
    random.seed(time.time() + process_id)
    try:
        for _ in range(num_requests):
            duration = random.uniform(0.5, 2.0)
            
            # Request the critical section
            rm.request_access(process_id, priority, node_id)
            
            # Perform actual work (critical section)
            time.sleep(duration)
            
            # Release the critical section
            rm.release_access(process_id, priority, node_id, duration)

            # Wait until the next request
            time.sleep(random.uniform(*delay_range))
            
    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")

def run_simulation():
    print("=== LOCK COHORTING RESOURCE ALLOCATION SIMULATION ===")
    manager = mp.Manager()
    
    # Handoff threshold = 10, starvation threshold = 10.0 seconds
    rm = ResourceManager(manager=manager, threshold=10, starvation_threshold=10.0)

    # Process specs: (process_id, node_id, priority, num_requests)
    specs = [
        (1, 0, Priority.HIGH, 5),
        (2, 0, Priority.MEDIUM, 4),
        (3, 0, Priority.LOW, 3),
        (4, 1, Priority.HIGH, 5),
        (5, 1, Priority.LOW, 6),
        (6, 1, Priority.MEDIUM, 4)
    ]

    processes = []
    for pid, nid, prio, reqs in specs:
        p = mp.Process(
            target=worker_process,
            args=(pid, nid, prio, reqs, rm, (0.5, 1.5))
        )
        processes.append(p)

    # Start the simulation
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    # --- Analyze and print statistics ---
    wait_times_list = list(rm.wait_times)
    starvation_list = list(rm.starvation_events)
    
    total_requests = len(wait_times_list)
    by_priority = {p.name: 0 for p in Priority}
    wait_sum_by_priority = {p.name: 0.0 for p in Priority}
    
    for _, prio_name, wait_time in wait_times_list:
        by_priority[prio_name] += 1
        wait_sum_by_priority[prio_name] += wait_time

    avg_wait_time = sum(w for _, _, w in wait_times_list) / total_requests if total_requests else 0.0
    
    print("\n=== STATISTICS ===")
    print(f"Total requests     : {total_requests}")
    print(f"Completed          : {total_requests}")
    print("Requests by priority:")
    for p in Priority:
        print(f"  - {p.name}: {by_priority[p.name]}")
        
    print(f"Average wait time  : {avg_wait_time:.2f}s")
    print("Average wait by priority:")
    for p in Priority:
        count = by_priority[p.name]
        avg = wait_sum_by_priority[p.name] / count if count else 0.0
        print(f"  - {p.name}: {avg:.2f}s")
        
    print(f"Starving requests  : {len(starvation_list)}")
    if starvation_list:
        print("Starving events detected:")
        for pid, prio, wait in starvation_list:
            print(f"  - Process #{pid} ({prio}) waited {wait:.2f}s")
    else:
        print("  - No starvation detected (fairness successfully ensured through Lock Cohorting)")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    run_simulation()
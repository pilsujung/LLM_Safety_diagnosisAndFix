import multiprocessing as mp
import time

class ResourceAllocator:
    def __init__(self, manager, threshold=10):
        """
        Initialize the Lock Cohorting manager for a virtual NUMA environment.
        Store only proxy objects created by Manager to prevent serialization (pickling) errors.
        """
        # Lock for protecting state
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
        self.threshold = threshold

        # Proxies for state management and logging from the original s5.py
        self.access_count = manager.dict({
            'priority_thread_0': 0,
            'priority_thread_1': 0,
            'starved_thread_0': 0,
            'starved_thread_1': 0
        })
        self.resource_usage_log = manager.list()

    def acquire(self, node_id):
        """Resource access request (Lock Cohorting Acquire)"""
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

    def release(self, node_id):
        """Resource access release (Lock Cohorting Release)"""
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

def worker_process(thread_name, node_id, allocator, target_count, work_time, sleep_time):
    """
    Global worker function assigned to a virtual NUMA node that accesses the resource using Lock Cohorting
    """
    count = 0
    while count < target_count:
        # Acquire the lock using the Cohorting method (prevents starvation)
        allocator.acquire(node_id)
        try:
            # Critical section
            current_count = allocator.access_count[thread_name] + 1
            allocator.access_count[thread_name] = current_count
            
            log_msg = f"{thread_name} (Node {node_id}) accessed at count {current_count}"
            allocator.resource_usage_log.append(log_msg)
            print(f"{thread_name} (Node {node_id}) accessed resource {current_count} times")
            
            time.sleep(work_time)
            count += 1
        finally:
            # Release the lock using the Cohorting method
            allocator.release(node_id)
            
        # Wait to simulate contention
        time.sleep(sleep_time)

def simulate_starvation_resolution():
    manager = mp.Manager()
    
    # Set the handoff threshold to 10
    allocator = ResourceAllocator(manager=manager, threshold=10)

    processes = []
    
    # Virtual NUMA node 0 group: processes that tend to monopolize the resource due to higher priority
    for i in range(2):
        name = f'priority_thread_{i}'
        p = mp.Process(target=worker_process, args=(name, 0, allocator, 50, 0.01, 0.005))
        processes.append(p)

    # Virtual NUMA node 1 group: processes that tended to experience starvation
    for i in range(2):
        name = f'starved_thread_{i}'
        p = mp.Process(target=worker_process, args=(name, 1, allocator, 50, 0.01, 0.02))
        processes.append(p)

    print("Starting Lock Cohorting simulation (starvation prevention test)...")
    
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    print("\nFinal access count (fairness result):")
    for k, v in dict(allocator.access_count).items():
        print(f"  {k}: {v}")

    # Print only some of the recorded logs (to avoid excessive output)
    print("\nResource usage log (first 10 & last 10):")
    logs = list(allocator.resource_usage_log)
    for log in logs[:10]:
        print(log)
    print("...")
    for log in logs[-10:]:
        print(log)

if __name__ == "__main__":
    # Force the spawn method for Windows compatibility and serialization safety
    mp.set_start_method('spawn', force=True)
    simulate_starvation_resolution()
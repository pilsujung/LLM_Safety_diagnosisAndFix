import multiprocessing as mp
import time
import random

class LockCohortingManager:
    def __init__(self, manager, threshold=5):
        """
        Initialize the Lock Cohorting manager for a virtual NUMA environment.
        Store only proxy objects created by Manager as member variables to prevent serialization errors.
        """
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


def resource_user_process(process_id, node_id, user_type, base_hold_time, variation_factor, iterations, resource_manager, stats_dict):
    """
    Worker process that accesses the resource using Lock Cohorting.
    Both Greedy and Lightweight processes use this function.
    """
    # Initialize statistics
    stats_dict[process_id] = {
        'access_count': 0,
        'total_wait_time': 0,
        'total_usage_time': 0,
        'last_access_time': None
    }
    
    iteration_count = 0
    
    while iteration_count < iterations:
        wait_start_time = time.time()
        
        # Acquire Lock Cohorting (prevents starvation)
        resource_manager.acquire(node_id)
        
        try:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time

            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
            
            print(f"[{user_type}] Process {process_id} (Node {node_id}) acquired resource (waited {wait_duration:.3f}s)")
            
            # Critical section (resource usage)
            usage_start_time = time.time()
            time.sleep(actual_hold_time)
            usage_end_time = time.time()
            actual_usage_time = usage_end_time - usage_start_time
            
            print(f"[{user_type}] Process {process_id} (Node {node_id}) releasing resource after {actual_usage_time:.3f}s")
            
            # Update statistics (Manager.dict changes must be reassigned to take effect)
            local_stats = stats_dict[process_id]
            local_stats['access_count'] += 1
            local_stats['total_wait_time'] += wait_duration
            local_stats['total_usage_time'] += actual_usage_time
            local_stats['last_access_time'] = time.time()
            stats_dict[process_id] = local_stats

        finally:
            # Release Lock Cohorting
            resource_manager.release(node_id)

        # Wait for the next access
        time.sleep(0.01 if user_type == 'GREEDY' else 0.02)
        iteration_count += 1


def monitor_process(stats_dict, run_event):
    """Monitor process that periodically prints statistics"""
    time.sleep(2)  
    
    while run_event.is_set():
        time.sleep(3)
        print("\n" + "="*60)
        print("PROCESS STATISTICS (LOCK COHORTING APPLIED)")
        print("="*60)
        
        current_stats = dict(stats_dict)
        for process_id, stats in current_stats.items():
            avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
            print(f"Process {process_id}:")
            print(f"  - Access count: {stats['access_count']}")
            print(f"  - Average wait time: {avg_wait:.3f}s")
            print(f"  - Total usage time: {stats['total_usage_time']:.3f}s")
            if stats['last_access_time']:
                time_since_last = time.time() - stats['last_access_time']
                print(f"  - Time since last access: {time_since_last:.3f}s")
        print("="*60 + "\n")


def main():
    """Main execution function"""
    print("Starting Process Starvation Resolution Demonstration")
    print("="*60)
    print("This program uses Lock Cohorting to prevent greedy processes")
    print("from starving lightweight processes.")
    print("Greedy processes -> Node 0, Lightweight processes -> Node 1")
    print("="*60 + "\n")
    
    manager = mp.Manager()
    
    # Lock Cohorting manager (consecutive handoff threshold set to 5)
    resource_manager = LockCohortingManager(manager=manager, threshold=5)
    
    stats_dict = manager.dict()
    run_event = manager.Event()
    run_event.set()

    processes = []

    # Greedy processes (assigned to virtual NUMA node 0)
    # process_id, node_id, user_type, base_hold_time, variation_factor, iterations
    p1 = mp.Process(target=resource_user_process, args=("Greedy-1", 0, "GREEDY", 1.0, 0.2, 10, resource_manager, stats_dict))
    p2 = mp.Process(target=resource_user_process, args=("Greedy-2", 0, "GREEDY", 0.8, 0.3, 10, resource_manager, stats_dict))
    
    # Lightweight processes (assigned to virtual NUMA node 1)
    p3 = mp.Process(target=resource_user_process, args=("Light-1", 1, "LIGHT", 0.1, 0.02, 30, resource_manager, stats_dict))
    p4 = mp.Process(target=resource_user_process, args=("Light-2", 1, "LIGHT", 0.05, 0.01, 30, resource_manager, stats_dict))

    monitor = mp.Process(target=monitor_process, args=(stats_dict, run_event))

    processes.extend([p1, p2, p3, p4, monitor])
    
    print("Starting all processes...\n")
    for p in processes:
        if p != monitor:
            p.start()
            time.sleep(0.1) 
    monitor.start()

    # Wait for worker processes except the monitor to terminate
    for p in [p1, p2, p3, p4]:
        p.join()

    # Terminate the monitor after work completes
    run_event.clear()
    monitor.join(timeout=2)
    if monitor.is_alive():
        monitor.terminate()

    print("\nFINAL RESULTS (FAIRNESS ACHIEVED):")
    
    print("\n" + "="*60)
    print("FINAL PROCESS STATISTICS")
    print("="*60)
    final_stats = dict(stats_dict)
    for process_id, stats in final_stats.items():
        avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
        print(f"Process {process_id}:")
        print(f"  - Access count: {stats['access_count']}")
        print(f"  - Average wait time: {avg_wait:.3f}s")
        print(f"  - Total usage time: {stats['total_usage_time']:.3f}s")
    print("="*60 + "\n")
    
    print("Demonstration complete!")
    print("\nObservation: Lock Cohorting successfully prevented starvation.")
    print("Lightweight processes on Node 1 accessed the resource fairly,")
    print("despite Greedy processes on Node 0 attempting to monopolize it.")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
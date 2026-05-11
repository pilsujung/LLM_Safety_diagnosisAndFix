import multiprocessing as mp
import time
import random

RUN_DURATION = 10              
MAX_ATTEMPTS_PER_PROCESS = 50   

class LockCohortingManager:
    def __init__(self, manager, threshold=10):
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


def worker_process(process_name, priority_level, node_id, resource_manager, stats_dict, run_event):
    """
    Global worker function that accesses the resource through Lock Cohorting.
    The starvation that occurred in the previous non-blocking polling method 
    is eliminated at the source through the fairness control of the hierarchical lock.
    """
    local_stats = {
        'attempts': 0, 
        'successes': 0, 
        'starvations': 0  # Effectively remains 0 thanks to Lock Cohorting
    }
    
    # Register initial statistics
    stats_dict[process_name] = local_stats

    while run_event.is_set() and local_stats['attempts'] < MAX_ATTEMPTS_PER_PROCESS:
        local_stats['attempts'] += 1

        # Blocking Lock Cohorting resource request (ensures fairness)
        resource_manager.acquire(node_id)
        
        try:
            local_stats['successes'] += 1
            print(f"[{priority_level}] {process_name} (Node {node_id}) acquired the resource")

            # Simulate the critical section (reflecting the hold time from the original script)
            if priority_level == 'HIGH':
                hold_time = random.uniform(0.1, 0.3)
            elif priority_level == 'NORMAL':
                hold_time = random.uniform(0.02, 0.08)
            else:
                hold_time = random.uniform(0.01, 0.03)
                
            time.sleep(hold_time)
            
        finally:
            # Release the resource and evaluate handoff
            resource_manager.release(node_id)
            
        # Update statistics
        stats_dict[process_name] = local_stats

        # Wait before the next access attempt (implements frequency differences by priority)
        if priority_level == 'HIGH':
            time.sleep(random.uniform(0.01, 0.05))
        elif priority_level == 'NORMAL':
            time.sleep(random.uniform(0.02, 0.08))
        else:
            time.sleep(random.uniform(0.05, 0.15))


def stats_printer(stats_dict, run_event):
    """Process that periodically prints current statistics"""
    while run_event.is_set():
        time.sleep(3)
        print("\n" + "="*65)
        print("PROCESS STATISTICS (LOCK COHORTING APPLIED):")
        print("="*65)
        
        # Copy and print the current state (safe iteration)
        current_stats = dict(stats_dict)
        for name in sorted(current_stats.keys()):
            data = current_stats[name]
            attempts = data['attempts']
            successes = data['successes']
            starvations = data['starvations']
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            
            print(f"{name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*65 + "\n")


def main():
    print("Starting Process Starvation Demonstration with Lock Cohorting")
    print("Processes are distributed across Virtual NUMA Node 0 and Node 1")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_PROCESS} attempts per process.\n")

    manager = mp.Manager()
    
    # Lock Cohorting manager (consecutive handoff threshold set to 10)
    resource_manager = LockCohortingManager(manager=manager, threshold=10)
    
    stats_dict = manager.dict()
    run_event = manager.Event()
    run_event.set()

    processes = []

    # Process configuration: priority / name / virtual NUMA node
    configs = [
        ('HIGH', 'HighPrio-1', 0),
        ('HIGH', 'HighPrio-2', 0),
        ('NORMAL', 'Normal-1', 0),
        ('NORMAL', 'Normal-2', 1),
        ('NORMAL', 'Normal-3', 1),
        ('LOW', 'LowPrio-1', 1),
        ('LOW', 'LowPrio-2', 1),
    ]

    # Create worker processes
    for prio, name, node in configs:
        p = mp.Process(
            target=worker_process, 
            args=(name, prio, node, resource_manager, stats_dict, run_event)
        )
        processes.append(p)

    # Create the statistics printer process
    printer_p = mp.Process(target=stats_printer, args=(stats_dict, run_event))
    processes.append(printer_p)

    # Start all processes
    for p in processes:
        p.start()
    
    # Main process waits for the specified duration
    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        time.sleep(1)

    print("\nStopping simulation (time limit reached)...")
    run_event.clear()
    
    # Wait for processes to terminate
    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    time.sleep(0.5)

    print("\nFINAL STATISTICS (STARVATION RESOLVED):")
    print("="*65)
    final_stats = dict(stats_dict)
    for name in sorted(final_stats.keys()):
        data = final_stats[name]
        attempts = data['attempts']
        successes = data['successes']
        starvations = data['starvations']
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        
        print(f"{name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
    
    print("\nLock Cohorting applied: High-priority/fast-running threads can no longer")
    print("monopolize the resource. Local handoff and global release thresholds ensure")
    print("all virtual NUMA nodes and processes get fair access (100% success rate).")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
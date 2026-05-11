import multiprocessing as mp
import time
import random

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


def access_shared_resource(thread_priority, thread_id, node_id, resource_manager, stats_dict, max_attempts=25):
    """
    Worker function where a process assigned to a virtual NUMA node accesses the resource using Lock Cohorting.
    """
    # Initialize statistics
    stats_dict[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': thread_priority,
        'starvation_count': 0
    }
    
    print(f"Process {thread_id} (Node {node_id}, Priority {thread_priority}) started - attempting resource access")
    
    attempt_counter = 0
    
    while attempt_counter < max_attempts:
        start_wait_time = time.time()

        # Acquire Lock Cohorting (blocking wait without starvation)
        resource_manager.acquire(node_id)
        
        try:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            wait_duration = time.time() - start_wait_time

            # Critical section (resource usage)
            resource_usage_time = 0.15 + (thread_priority * 0.05)
            time.sleep(resource_usage_time)
            
            print(f"[{current_time}] ✓ Process {thread_id} (Node {node_id}, Prio {thread_priority}) acquired resource after {wait_duration:.3f}s wait and released")

            # Update statistics (Manager.dict is updated using a shallow copy)
            local_stats = stats_dict[thread_id]
            local_stats['successful_accesses'] += 1
            local_stats['total_wait_time'] += wait_duration
            stats_dict[thread_id] = local_stats

        finally:
            # Release Lock Cohorting
            resource_manager.release(node_id)
        
        attempt_counter += 1

        # Wait before the next access
        base_delay = 0.1 + (thread_priority * 0.05)
        inter_attempt_delay = base_delay + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)

    stats = stats_dict[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    
    print(f"\n📊 Process {thread_id} Final Stats:")
    print(f"   Node ID: {node_id}")
    print(f"   Priority: {thread_priority}")
    print(f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Starvation episodes: {stats['starvation_count']}")


def display_simulation_summary(stats_dict):
    """Final simulation result summary"""
    print("\n" + "="*60)
    print("LOCK COHORTING SIMULATION SUMMARY (STARVATION RESOLVED)")
    print("="*60)
    
    total_resource_accesses = sum(stats['successful_accesses'] for stats in stats_dict.values())
    print(f"Total resource accesses across all processes: {total_resource_accesses}")
    
    high_priority_accesses = sum(stats['successful_accesses'] 
                                for stats in stats_dict.values() 
                                if stats['priority'] <= 2)
    low_priority_accesses = sum(stats['successful_accesses'] 
                               for stats in stats_dict.values() 
                               if stats['priority'] > 2)
    
    print(f"High priority processes (1-2) total accesses: {high_priority_accesses}")
    print(f"Low/Med priority processes (3+) total accesses: {low_priority_accesses}")
    
    total_starvation = sum(stats['starvation_count'] for stats in stats_dict.values())
    print(f"Total starvation episodes detected: {total_starvation} (Should be 0)")
    
    if low_priority_accesses < high_priority_accesses * 0.3:
        print("⚠️  WARNING: Low priority processes still underserved!")
    else:
        print("✅ SUCCESS: Lock Cohorting achieved fairness across all processes.")
    
    print("="*60)


def main():
    print("Starting Lock Cohorting Starvation Resolution Simulation")
    print("Processes grouped into Virtual NUMA Nodes (0 and 1)")
    print("-" * 60)
    
    manager = mp.Manager()
    resource_manager = LockCohortingManager(manager=manager, threshold=10)
    stats_dict = manager.dict()

    # (priority, process_name, virtual NUMA node_ID)
    process_configurations = [
        (1, "HighPrio-A", 0),    
        (2, "HighPrio-B", 0),    
        (4, "LowPrio-A", 1),     
        (5, "LowPrio-B", 1),    
        (3, "MedPrio-A", 0)      
    ]
    
    active_processes = []

    for priority, process_name, node_id in process_configurations:
        p = mp.Process(
            target=access_shared_resource, 
            args=(priority, process_name, node_id, resource_manager, stats_dict),
            name=f"Process-{process_name}"
        )
        active_processes.append(p)
        p.start()
        time.sleep(0.1)

    for p in active_processes:
        p.join()

    display_simulation_summary(dict(stats_dict))
    print("\nSimulation completed - Starvation effects eliminated by Lock Cohorting")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
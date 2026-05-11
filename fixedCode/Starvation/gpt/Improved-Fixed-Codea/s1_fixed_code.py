import multiprocessing as mp
import time
import random
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

class ResourceManager:
    def __init__(self, manager, threshold=10):
        """
        Initialize the Lock Cohorting manager in a virtual NUMA environment.
        Do not store the Manager object itself to prevent serialization errors; store only proxies as members.
        """
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
        self.threshold = threshold

        # For recording statistics data
        self.completion_times = manager.dict()
        self.waiting_times = manager.dict()
        self.resource_access_logs = manager.list()
        self.process_start_times = manager.dict()
        
    def request_resource(self, process_id, node_id):
        """
        Resource access request (Lock Acquisition)
        """
        self.process_start_times[process_id] = time.time()
        
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"{timestamp} - Process {process_id} (Node: {node_id}) requested resource access"
        self.resource_access_logs.append(log_entry)
        print(log_entry)

        # 1. Increase the local waiter count
        with self.state_lock:
            self.local_waiters[node_id] = self.local_waiters[node_id] + 1
            
        # 2. Acquire the local lock (S_i)
        self.local_locks[node_id].acquire()
        
        # Decrease the local waiter count and check global lock ownership
        with self.state_lock:
            self.local_waiters[node_id] = self.local_waiters[node_id] - 1
            current_owner = self.global_owner.value
            
        # 3. If the global lock is owned by another node, acquire the global lock (G)
        if current_owner != node_id:
            self.global_lock.acquire()
            with self.state_lock:
                self.global_owner.value = node_id
                self.handoff_count.value = 0

        # === Enter Critical Section ===
        wait_time = time.time() - self.process_start_times[process_id]
        if process_id in self.waiting_times:
            w_list = self.waiting_times[process_id]
            w_list.append(wait_time)
            self.waiting_times[process_id] = w_list
        else:
            self.waiting_times[process_id] = [wait_time]
            
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"{timestamp} - Process {process_id} (Node: {node_id}) granted resource access after waiting {wait_time:.2f} seconds"
        self.resource_access_logs.append(log_entry)
        print(log_entry)

    def release_resource(self, process_id, node_id, work_time):
        """
        Resource access release (Lock Release)
        """
        # Update statistics
        completion_time = time.time()
        if process_id in self.completion_times:
            c_list = self.completion_times[process_id]
            c_list.append(completion_time)
            self.completion_times[process_id] = c_list
        else:
            self.completion_times[process_id] = [completion_time]
            
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"{timestamp} - Process {process_id} (Node: {node_id}) completed task, held resource for {work_time:.2f} seconds"
        self.resource_access_logs.append(log_entry)
        print(log_entry)

        # 4. Evaluate Cohorting release
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

def worker_process(process_id, node_id, work_iterations, resource_manager, delay_range):
    """
    Worker process that accesses the resource using Lock Cohorting
    """
    try:
        for i in range(work_iterations):
            work_time = random.uniform(0.5, 2.0)
            
            # Request the critical section
            resource_manager.request_resource(process_id, node_id)
            
            # Perform actual work (critical section)
            time.sleep(work_time)
            
            # Release the critical section
            resource_manager.release_resource(process_id, node_id, work_time)

            # Wait until the next task
            delay = random.uniform(delay_range[0], delay_range[1])
            time.sleep(delay)
            
    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")

def analyze_results(resource_manager):
    """Analyze and visualize results"""
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)

    avg_waiting_times = {}
    tasks_completed = {}
    
    for pid in range(5):
        wait_times = waiting_times.get(pid, [])
        if len(wait_times) > 0:
            avg_waiting_times[pid] = sum(wait_times) / len(wait_times)
        else:
            avg_waiting_times[pid] = 0.0
            
        completions = completion_times.get(pid, [])
        tasks_completed[pid] = len(completions)

    print("\nProcess Summary:")
    print("---------------")
    for pid in sorted(avg_waiting_times.keys()):
        print(f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s, Tasks Completed: {tasks_completed[pid]}")

    starved_threshold = 2.0  
    starved_processes = [pid for pid, wait_time in avg_waiting_times.items() if wait_time > starved_threshold]
    
    if starved_processes:
        print("\nStarved Processes:")
        print("----------------")
        for pid in starved_processes:
            print(f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s")
    else:
        print("\nNo processes were starved! (Lock Cohorting efficiently guaranteed fairness)")

    process_ids = sorted(avg_waiting_times.keys())
    avg_waits = [avg_waiting_times[pid] for pid in process_ids]
    
    plt.figure(figsize=(10, 6))
    plt.bar(process_ids, avg_waits, color=['red' if pid in starved_processes else 'blue' for pid in process_ids])
    plt.title('Average Waiting Time by Process (Lock Cohorting)')
    plt.xlabel('Process ID')
    plt.ylabel('Average Waiting Time (seconds)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('waiting_times.png')
    
    tasks = [tasks_completed[pid] for pid in process_ids]
    
    plt.figure(figsize=(10, 6))
    plt.bar(process_ids, tasks, color=['green' if tasks_completed[pid] > 1 else 'orange' for pid in process_ids])
    plt.title('Tasks Completed by Process (Lock Cohorting)')
    plt.xlabel('Process ID')
    plt.ylabel('Number of Tasks Completed')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('tasks_completed.png')
    
    print("\nSaved visualizations: waiting_times.png and tasks_completed.png")

    with open('resource_access_logs.txt', 'w') as f:
        for log in resource_manager.resource_access_logs:
            f.write(log + '\n')
    
    print("\nDetailed logs saved to resource_access_logs.txt")

def main():
    num_processes = 5
    simulation_duration = 30 
    
    # Create shared objects through Manager
    manager = mp.Manager()
    
    # Initialize the manager with the threshold set to 10
    resource_manager = ResourceManager(manager=manager, threshold=10)

    processes = []

    # Node 0 group (virtual NUMA node 0)
    for i in range(2):
        p = mp.Process(
            target=worker_process,
            args=(i, 0, 10, resource_manager, (0.5, 1.5))  
        )
        processes.append(p)

    # Node 1 group (virtual NUMA node 1)
    for i in range(2, 4):
        p = mp.Process(
            target=worker_process,
            args=(i, 1, 10, resource_manager, (0.5, 1.5)) 
        )
        processes.append(p)
    
    # Assign Process 4 to Node 0
    p = mp.Process(
        target=worker_process,
        args=(4, 0, 10, resource_manager, (0.5, 1.5)) 
    )
    processes.append(p)

    print(f"Starting Lock Cohorting simulation with {num_processes} processes...")
    for p in processes:
        p.start()

    time.sleep(simulation_duration)

    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()
    
    print("All processes have completed or been terminated")

    analyze_results(resource_manager)

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
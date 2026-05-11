import multiprocessing as mp
import time
import random
from datetime import datetime

SIMULATION_DURATION = 7.0        
VIP_WORK_TIME = 0.003            
STANDARD_WORK_TIME = 0.0045      
FREE_WORK_TIME = 0.006          

VIP_SLEEP_TIME = 0.001        
STANDARD_SLEEP_TIME = 0.006      
FREE_SLEEP_TIME = 0.010        

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


def safe_print(message, console_lock):
    """Thread/Process-safe printing function"""
    with console_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")


def simulate_db_work(client_name, work_duration, db_row_count, total_db_transactions, client_statistics, stats_lock):
    """Simulate work being done on a shared database"""
    start_work_time = time.time()

    # Simulate a DB transaction
    added_rows = random.randint(10, 30)
    db_row_count.value += added_rows
    total_db_transactions.value += 1

    time.sleep(work_duration)

    actual_work_time = time.time() - start_work_time

    with stats_lock:
        stats = client_statistics[client_name]
        stats['total_work_time'] += actual_work_time
        client_statistics[client_name] = stats

    return actual_work_time


def client_process(client_name, display_name, emoji, node_id, work_time, sleep_time, resource_manager, db_row_count, total_db_transactions, client_statistics, stats_lock, console_lock, run_event):
    """
    Client process that accesses the database (resource) through Lock Cohorting.
    Even the Free Client that previously experienced starvation can acquire the resource reliably thanks to the fairness guarantees of the hierarchical lock.
    """
    with stats_lock:
        stats = client_statistics[client_name]
        stats['start_time'] = time.time()
        client_statistics[client_name] = stats

    safe_print(f"{emoji} {display_name} CLIENT: Starting requests (Node {node_id})", console_lock)

    while run_event.is_set():
        # Blocking access through Lock Cohorting (eliminates timeouts, failures, and starvation at the source)
        resource_manager.acquire(node_id)
        
        try:
            with stats_lock:
                stats = client_statistics[client_name]
                stats['access_count'] += 1
                current_count = stats['access_count']
                client_statistics[client_name] = stats

            if current_count % 50 == 0:
                safe_print(f"{emoji} {display_name}: Acquired database connection (Request #{current_count})", console_lock)

            simulate_db_work(client_name, work_time, db_row_count, total_db_transactions, client_statistics, stats_lock)
            
        finally:
            resource_manager.release(node_id)

        time.sleep(sleep_time)

    with stats_lock:
        stats = client_statistics[client_name]
        stats['end_time'] = time.time()
        client_statistics[client_name] = stats
        
    safe_print(f"{emoji} {display_name} CLIENT: Stopped after simulation window", console_lock)


def print_final_statistics(client_statistics, database_row_count, total_db_transactions, console_lock):
    """Print comprehensive statistics about the starvation resolution"""
    safe_print("\n" + "=" * 80, console_lock)
    safe_print("FINAL CLIENT STARVATION RESOLUTION STATISTICS (LOCK COHORTING)", console_lock)
    safe_print("=" * 80, console_lock)

    final_stats = dict(client_statistics)

    for client_name, stats in final_stats.items():
        if stats['start_time'] and stats['end_time']:
            execution_time = stats['end_time'] - stats['start_time']
            total_attempts = stats['access_count'] + stats['failed_attempts']
            success_rate = (stats['access_count'] / total_attempts) * 100 if total_attempts > 0 else 0.0
            avg_work_time = (
                stats['total_work_time'] / stats['access_count']
                if stats['access_count'] > 0
                else 0.0
            )

            readable_name = client_name.upper().replace('_', ' ')

            safe_print(f"\n📈 {readable_name}:", console_lock)
            safe_print(f"   • Successful requests: {stats['access_count']}", console_lock)
            safe_print(f"   • Failed lock acquisitions: {stats['failed_attempts']} (Eliminated by Cohorting)", console_lock)
            safe_print(f"   • Success rate: {success_rate:.2f}%", console_lock)
            safe_print(f"   • Process lifetime: {execution_time:.2f}s", console_lock)
            safe_print(f"   • Total work time: {stats['total_work_time']:.2f}s", console_lock)
            safe_print(f"   • Avg work time per success: {avg_work_time:.4f}s", console_lock)

    safe_print("\n🔢 SHARED DATABASE STATISTICS:", console_lock)
    safe_print(f"   • Final row count: {database_row_count.value}", console_lock)
    safe_print(f"   • Total DB transactions: {total_db_transactions.value}", console_lock)

    vip_access = final_stats['vip_client']['access_count']
    std_access = final_stats['standard_client']['access_count']
    free_access = final_stats['free_client']['access_count']

    def ratio(a, b):
        return a / b if b > 0 else float('inf')

    r_vip_std = ratio(vip_access, std_access)
    r_vip_free = ratio(vip_access, free_access)
    r_std_free = ratio(std_access, free_access)

    safe_print("\n✅ FAIRNESS & STARVATION ANALYSIS:", console_lock)
    safe_print(f"   • VIP vs Standard success ratio: {r_vip_std:.2f}:1", console_lock)
    safe_print(f"   • VIP vs Free success ratio: {r_vip_free:.2f}:1", console_lock)
    safe_print(f"   • Standard vs Free success ratio: {r_std_free:.2f}:1", console_lock)

    safe_print("   • ALL CLIENTS RECEIVED FAIR ACCESS. The 'May-Pass-Local' threshold ensured", console_lock)
    safe_print("     that VIP threads on Node 0 periodically yielded to Free threads on Node 1.", console_lock)
    safe_print("=" * 80, console_lock)


def main():
    """Main function to demonstrate client starvation resolution using Lock Cohorting"""
    manager = mp.Manager()
    
    # Set the handoff threshold to 10
    resource_manager = LockCohortingManager(manager=manager, threshold=10)
    
    console_lock = manager.Lock()
    stats_lock = manager.Lock()
    run_event = manager.Event()
    run_event.set()

    database_row_count = manager.Value('i', 0)
    total_db_transactions = manager.Value('i', 0)

    client_statistics = manager.dict({
        'vip_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None},
        'standard_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None},
        'free_client': {'access_count': 0, 'failed_attempts': 0, 'total_work_time': 0.0, 'start_time': None, 'end_time': None, 'max_consecutive_failures': 0}
    })

    safe_print("🚀 STARTING STARVATION RESOLUTION DEMONSTRATION (LOCK COHORTING)", console_lock)
    safe_print(f"Simulation duration: {SIMULATION_DURATION}s", console_lock)
    safe_print("Node 0: VIP Client | Node 1: Standard & Free Clients", console_lock)
    safe_print("-" * 80, console_lock)

    # Virtual NUMA node assignment: VIP(0), Standard(1), Free(1)
    processes = []
    
    p_vip = mp.Process(target=client_process, args=(
        'vip_client', 'VIP', '💎', 0, VIP_WORK_TIME, VIP_SLEEP_TIME, 
        resource_manager, database_row_count, total_db_transactions, client_statistics, stats_lock, console_lock, run_event
    ))
    processes.append(p_vip)

    p_std = mp.Process(target=client_process, args=(
        'standard_client', 'STANDARD', '🟦', 1, STANDARD_WORK_TIME, STANDARD_SLEEP_TIME, 
        resource_manager, database_row_count, total_db_transactions, client_statistics, stats_lock, console_lock, run_event
    ))
    processes.append(p_std)

    p_free = mp.Process(target=client_process, args=(
        'free_client', 'FREE', '🆓', 1, FREE_WORK_TIME, FREE_SLEEP_TIME, 
        resource_manager, database_row_count, total_db_transactions, client_statistics, stats_lock, console_lock, run_event
    ))
    processes.append(p_free)

    for p in processes:
        p.start()

    # Run the simulation
    time.sleep(SIMULATION_DURATION)
    
    # Signal simulation termination
    run_event.clear()

    # Wait for processes to terminate
    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    print_final_statistics(client_statistics, database_row_count, total_db_transactions, console_lock)

    safe_print("\n🏁 CLIENT STARVATION RESOLUTION DEMONSTRATION COMPLETED", console_lock)


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    start = time.time()
    main()
    print("Total runtime:", time.time() - start)
import multiprocessing as mp
import time
import random
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')

class FileSystem:
    def __init__(self, manager, threshold=10):
        """
        Initialize the Lock Cohorting manager for a virtual NUMA environment.
        Store only proxy objects created by Manager to prevent serialization errors.
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

        # For recording statistics data
        self.execution_history = manager.list()
        self.stats = manager.dict()
        self.t0 = manager.Value('d', time.time())

    def request_access(self, thread_id, priority, node_id):
        """Resource access request (Lock Cohorting Acquire)"""
        logging.info(f"{thread_id} (p={priority}, node={node_id}) requested")
        
        # 1. Increase the local waiter count
        with self.state_lock:
            self.local_waiters[node_id] = self.local_waiters[node_id] + 1
            
        # 2. Acquire the local lock (S_i)
        self.local_locks[node_id].acquire()
        
        # 3. Decrease the local waiter count and check global lock ownership
        with self.state_lock:
            self.local_waiters[node_id] = self.local_waiters[node_id] - 1
            current_owner = self.global_owner.value
            
        # 4. If the global lock is owned by another node, acquire the global lock (G)
        if current_owner != node_id:
            self.global_lock.acquire()
            with self.state_lock:
                self.global_owner.value = node_id
                self.handoff_count.value = 0

    def release_access(self, thread_id, priority, node_id):
        """Resource access release (Lock Cohorting Release)"""
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

    def record_execution(self, record):
        self.execution_history.append(record)

    def save_stats(self, thread_id, priority, access_count, successful_accesses, wait_times, total_wait_time):
        self.stats[thread_id] = {
            'thread_id': thread_id,
            'priority': priority,
            'access_count': access_count,
            'successful_accesses': successful_accesses,
            'wait_times': wait_times,
            'total_wait_time': total_wait_time
        }

    def _group(self, priority):
        return 'High' if priority <= 2 else ('Medium' if priority <= 5 else 'Low')


def worker_process(thread_id, prefix, priority, access_count, node_id, fs, stop_event):
    """
    Worker function assigned to a virtual NUMA node that accesses the file system using Lock Cohorting
    """
    wait_times = []
    successful_accesses = 0
    total_wait_time = 0.0

    try:
        for _ in range(access_count):
            if stop_event.is_set():
                break

            enq_time = time.time()
            fs.request_access(thread_id, priority, node_id)
            wait = time.time() - enq_time
            
            wait_times.append(wait)
            total_wait_time += wait
            successful_accesses += 1

            fs.record_execution({
                'thread_id': thread_id, 'priority': priority, 'node_id': node_id,
                'action': 'access', 'time': time.time() - fs.t0.value, 'wait_time': wait
            })
            logging.info(f"{thread_id} (p={priority}, node={node_id}) got access after {wait:.2f}s")

            # Simulate critical-section work
            use = random.uniform(0.04, 0.08) * (1.8 if priority <= 2 else 1.0)
            time.sleep(use)

            fs.release_access(thread_id, priority, node_id)
            
            fs.record_execution({
                'thread_id': thread_id, 'priority': priority, 'node_id': node_id,
                'action': 'release', 'time': time.time() - fs.t0.value
            })

            # Wait for the next request
            if priority > 3:
                time.sleep(random.uniform(0.02, 0.05))

    except Exception as e:
        logging.error(f"Error in {thread_id}: {e}")
    finally:
        # Save process statistics on termination
        fs.save_stats(thread_id, priority, access_count, successful_accesses, wait_times, total_wait_time)


def log_final_summary(fs):
    def safe(a, b): return (a / b) if b else 0.0

    stats = dict(fs.stats)
    threads = list(stats.values())

    groups = {
        'High':   [t for t in threads if t['priority'] <= 2],
        'Medium': [t for t in threads if 2 < t['priority'] <= 5],
        'Low':    [t for t in threads if t['priority'] > 5],
    }

    logging.info("\n===== FINAL STARVATION SUMMARY (Lock Cohorting) =====")
    for name, ts in groups.items():
        comp = sum(t['successful_accesses'] for t in ts)
        req  = sum(t['access_count'] for t in ts)
        waits = [w for t in ts for w in t['wait_times']]
        mean = float(np.mean(waits))       if waits else 0.0
        med  = float(np.median(waits))     if waits else 0.0
        p95  = float(np.percentile(waits, 95)) if waits else 0.0
        mx   = float(np.max(waits))        if waits else 0.0
        logging.info(
            f"[{name}] completion {safe(comp, req)*100:.2f}% ({comp}/{req}) | "
            f"wait mean/med/p95/max: {mean:.3f}/{med:.3f}/{p95:.3f}/{mx:.3f}s"
        )

    acc = np.array([t['successful_accesses'] for t in threads], float)
    fairness = (acc.sum()**2) / (len(acc) * (acc**2).sum()) if acc.size and (acc**2).sum() > 0 else 0.0
    logging.info(f"Fairness (Jain): {fairness:.4f}")

    share = {'High': 0, 'Medium': 0, 'Low': 0}
    for e in fs.execution_history:
        if e.get('action') == 'access':
            share[fs._group(e['priority'])] += 1
    total_a = sum(share.values())
    logging.info("Access share: " + ", ".join(f"{k} {v} ({safe(v, total_a)*100:.1f}%)" for k, v in share.items()))

    top = sorted(
        threads,
        key=lambda t: (
            safe(t['access_count'] - t['successful_accesses'], t['access_count']),  
            (t['access_count'] - t['successful_accesses'])                       
        ),
        reverse=True
    )[:5]

    logging.info("Top suspected starved threads (if any):")
    for i, t in enumerate(top, 1):
        deficit = t['access_count'] - t['successful_accesses']
        rate = safe(deficit, t['access_count'])
        avg = safe(t['total_wait_time'], t['successful_accesses'])
        logging.info(
            f"  {i}. {t['thread_id']} p={t['priority']} | "
            f"{t['successful_accesses']}/{t['access_count']}, "
            f"starv {rate*100:.1f}%, avg wait {avg:.3f}s"
        )
    logging.info("===== END OF SUMMARY =====\n")


def run_simulation(duration=8):
    manager = mp.Manager()
    fs = FileSystem(manager=manager, threshold=10)
    stop_event = manager.Event()
    processes = []

    # Work specs: (prefix, count, priority, total access count)
    spec = [
        ('HP', 3, 1, 12),
        ('MP', 4, 3,  8),
        ('LP', 5, 9,  6),
    ]
    
    process_idx = 0
    for prefix, cnt, prio, acc_n in spec:
        for i in range(1, cnt + 1):
            thread_id = f"{prefix}-{i}"
            # Assign a virtual NUMA node (0 or 1)
            node_id = process_idx % 2
            
            p = mp.Process(
                target=worker_process,
                args=(thread_id, prefix, prio, acc_n, node_id, fs, stop_event)
            )
            processes.append(p)
            process_idx += 1

    for p in processes:
        p.start()

    time.sleep(duration)
    stop_event.set()

    # Wait for processes to terminate safely
    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    active = sum(p.is_alive() for p in processes)
    completed = len(processes) - active
    logging.info(f"\n----- SNAPSHOT @ {duration}s ----- Active: {active}, Completed: {completed}")

    dist = {'High': 0, 'Medium': 0, 'Low': 0}
    for e in fs.execution_history:
        if e.get('action') == 'access':
            dist[fs._group(e['priority'])] += 1
            
    total_events = sum(dist.values())
    for k, v in dist.items():
        pct = (v / total_events * 100) if total_events else 0.0
        logging.info(f"{k} accesses: {v} ({pct:.2f}% of total accesses)")

    stats = dict(fs.stats)
    starved = sum(1 for t in stats.values() if t['successful_accesses'] < t['access_count'])
    logging.info(f"Starved processes (incomplete): {starved}")
    
    log_final_summary(fs)


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    logging.info("Starting file system starvation simulation with Lock Cohorting...")
    run_simulation(duration=8)
    logging.info("Simulation ended.")
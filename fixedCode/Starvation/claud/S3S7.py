import threading, time, random, logging
from queue import PriorityQueue
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')

class FileSystem:
    def __init__(self, aging_threshold=2.0, aging_boost=3):
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.waiting_threads = {}
        self.current_owner = None
        self.execution_history = [] 
        self.t0 = time.time()
        self.aging_threshold = aging_threshold
        self.aging_boost = aging_boost

    def _get_effective_priority(self, tid):
        """Calculate effective priority with aging"""
        if tid not in self.waiting_threads:
            return float('inf')
        
        original_priority, enqueue_time, _ = self.waiting_threads[tid]
        wait_time = time.time() - enqueue_time
        

        age_factor = int(wait_time / self.aging_threshold)
        effective_priority = max(1, original_priority - (age_factor * self.aging_boost))
        
        return effective_priority

    def _get_next_thread(self):
        """Get the thread with highest effective priority"""
        if not self.waiting_threads:
            return None
        

        best_tid = min(self.waiting_threads.keys(), 
                      key=lambda tid: (self._get_effective_priority(tid), 
                                      self.waiting_threads[tid][1]))
        return best_tid

    def request_access(self, thread_id, priority, stop_event: threading.Event):
        """Request exclusive access with aging to prevent starvation."""
        with self.cv:
            enq = time.time()
            self.waiting_threads[thread_id] = (priority, enq, priority)
            logging.info(f"{thread_id} (p={priority}) requested")
            
            while True:
                if stop_event.is_set():
                    self.waiting_threads.pop(thread_id, None)
                    return None
                
                if self.current_owner is None:
                    next_tid = self._get_next_thread()
                    if next_tid == thread_id:
                        _, t_enq, orig_priority = self.waiting_threads.pop(thread_id)
                        self.current_owner = thread_id
                        wait = time.time() - t_enq
                        
                        eff_priority = max(1, priority - int(wait / self.aging_threshold) * self.aging_boost)
                        
                        self.execution_history.append({
                            'thread_id': thread_id, 'priority': orig_priority,
                            'effective_priority': eff_priority,
                            'action': 'access', 'time': time.time() - self.t0, 'wait_time': wait
                        })
                        logging.info(f"{thread_id} (p={priority}, eff_p={eff_priority}) got access after {wait:.2f}s")
                        return wait
                
                self.cv.wait(timeout=0.2)

    def release_access(self, thread_id, priority):
        with self.cv:
            if self.current_owner == thread_id:
                self.current_owner = None
                self.execution_history.append({
                    'thread_id': thread_id, 'priority': priority,
                    'action': 'release', 'time': time.time() - self.t0
                })
                self.cv.notify_all()
            else:
                logging.warning(f"{thread_id} tried to release without ownership")

    def _group(self, priority):
        return 'High' if priority <= 2 else ('Medium' if priority <= 5 else 'Low')


class FileSystemThread(threading.Thread):
    def __init__(self, thread_id, fs: FileSystem, priority, access_count: int, stop_event: threading.Event):
        super().__init__()
        self.thread_id = thread_id
        self.fs = fs
        self.priority = priority
        self.access_count = access_count
        self.stop_event = stop_event
        self.total_wait_time = 0.0
        self.successful_accesses = 0
        self.wait_times = []

    def run(self):
        for _ in range(self.access_count):
            if self.stop_event.is_set():
                break
            wait = self.fs.request_access(self.thread_id, self.priority, self.stop_event)
            if wait is None:
                break
            self.total_wait_time += wait
            self.wait_times.append(wait)
            self.successful_accesses += 1

            use = random.uniform(0.04, 0.08) * (1.8 if self.priority <= 2 else 1.0)
            time.sleep(use)

            self.fs.release_access(self.thread_id, self.priority)

            if self.priority > 3:
                time.sleep(random.uniform(0.02, 0.05))


def log_final_summary(fs, threads):
    def safe(a, b): return (a / b) if b else 0.0

    groups = {
        'High':   [t for t in threads if t.priority <= 2],
        'Medium': [t for t in threads if 2 < t.priority <= 5],
        'Low':    [t for t in threads if t.priority > 5],
    }

    logging.info("\n===== FINAL STARVATION SUMMARY =====")
    for name, ts in groups.items():
        comp = sum(t.successful_accesses for t in ts)
        req  = sum(t.access_count for t in ts)
        waits = [w for t in ts for w in t.wait_times]
        mean = float(np.mean(waits))       if waits else 0.0
        med  = float(np.median(waits))     if waits else 0.0
        p95  = float(np.percentile(waits, 95)) if waits else 0.0
        mx   = float(np.max(waits))        if waits else 0.0
        logging.info(
            f"[{name}] completion {safe(comp, req)*100:.2f}% ({comp}/{req}) | "
            f"wait mean/med/p95/max: {mean:.3f}/{med:.3f}/{p95:.3f}/{mx:.3f}s"
        )

    acc = np.array([t.successful_accesses for t in threads], float)
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
            safe(t.access_count - t.successful_accesses, t.access_count),  
            (t.access_count - t.successful_accesses)                       
        ),
        reverse=True
    )[:5]

    logging.info("Top suspected starved threads:")
    for i, t in enumerate(top, 1):
        deficit = t.access_count - t.successful_accesses
        rate = safe(deficit, t.access_count)
        avg = safe(t.total_wait_time, t.successful_accesses)
        logging.info(
            f"  {i}. {t.thread_id} p={t.priority} | "
            f"{t.successful_accesses}/{t.access_count}, "
            f"starv {rate*100:.1f}%, avg wait {avg:.3f}s"
        )
    logging.info("===== END OF SUMMARY =====\n")

def run_simulation(duration=8):
    fs = FileSystem(aging_threshold=2.0, aging_boost=3)
    stop_event = threading.Event()
    threads = []

    spec = [
        ('HP', 3, 1, 12),
        ('MP', 4, 3,  8),
        ('LP', 5, 9,  6),
    ]
    for prefix, cnt, prio, acc_n in spec:
        for i in range(1, cnt + 1):
            threads.append(FileSystemThread(f"{prefix}-{i}", fs, prio, acc_n, stop_event))

    for t in threads:
        t.start()

    time.sleep(duration)
    stop_event.set()

    for t in threads:
        t.join(timeout=2)

    active = sum(t.is_alive() for t in threads)
    logging.info(f"\n----- SNAPSHOT @ {duration}s ----- Active: {active}, Completed: {len(threads) - active}")

    dist = {'High': 0, 'Medium': 0, 'Low': 0}
    for e in fs.execution_history:
        if e.get('action') == 'access':
            dist[fs._group(e['priority'])] += 1
    total_events = sum(dist.values())
    for k, v in dist.items():
        pct = (v / total_events * 100) if total_events else 0.0
        logging.info(f"{k} accesses: {v} ({pct:.2f}% of total accesses)")

    starved = sum(t.successful_accesses < t.access_count for t in threads)
    logging.info(f"Starved threads (incomplete): {starved}")
    log_final_summary(fs, threads)


if __name__ == "__main__":
    logging.info("Starting file system starvation simulation...")
    run_simulation(duration=8)
    logging.info("Simulation ended.")
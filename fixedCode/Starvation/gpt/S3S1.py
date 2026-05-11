import threading, time, random, logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')

AGING_RATE = 1.2

class FileSystem:
    def __init__(self):
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)

        self.waiting = []
        self.seq_counter = 0
        self.current_owner = None
        self.execution_history = []
        self.t0 = time.time()

    def _best_index(self, now: float) -> int | None:
        """Return index of best candidate per priority aging; None if empty."""
        if not self.waiting:
            return None

        def score(item):
            effective = item['prio'] - AGING_RATE * (now - item['enq'])
            return (effective, item['enq'], item['seq'])
        best_i = 0
        best_s = score(self.waiting[0])
        for i in range(1, len(self.waiting)):
            s = score(self.waiting[i])
            if s < best_s:
                best_i, best_s = i, s
        return best_i

    def request_access(self, thread_id, priority, stop_event: threading.Event):
        """Request exclusive access. Lower priority number = higher priority."""
        with self.cv:
            enq = time.time()
            self.seq_counter += 1
            self.waiting.append({'prio': priority, 'tid': thread_id, 'enq': enq, 'seq': self.seq_counter})
            logging.info(f"{thread_id} (p={priority}) requested")

            while True:
                if stop_event.is_set():

                    self.waiting[:] = [it for it in self.waiting if it['tid'] != thread_id]
                    return None

                if self.current_owner is None and self.waiting:
                    now = time.time()
                    best_i = self._best_index(now)
                    cand = self.waiting[best_i] if best_i is not None else None
                    if cand and cand['tid'] == thread_id:

                        self.waiting.pop(best_i)
                        self.current_owner = thread_id
                        wait = now - enq
                        self.execution_history.append({
                            'thread_id': thread_id, 'priority': priority,
                            'action': 'access', 'time': now - self.t0, 'wait_time': wait
                        })
                        logging.info(f"{thread_id} (p={priority}) got access after {wait:.2f}s")
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
    def pct(v): return (v / total_a * 100) if total_a else 0.0
    logging.info("Access share: " + ", ".join(f"{k} {v} ({pct(v):.1f}%)" for k, v in share.items()))

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
    fs = FileSystem()
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

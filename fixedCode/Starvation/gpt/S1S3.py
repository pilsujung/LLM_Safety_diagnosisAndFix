import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import threading
import matplotlib.pyplot as plt




class ResourceManager:
    def __init__(self, resource_units=1, aging_rate=0.5):
        """
        Args:
            resource_units: Number of concurrent resources available
            aging_rate: How quickly waiting time increases effective priority (prio/sec)
        """
        self.resource_units = resource_units
        self.aging_rate = aging_rate


        mgr = mp.Manager()
        self.completion_times = mgr.dict()
        self.waiting_times = mgr.dict()
        self.resource_access_logs = mgr.list()
        self.process_start_times = mgr.dict()


        self.process_queue = mp.Queue()

        self.wait_list = []

        self.active_processes = set()
        self.exit_flag = mp.Event()


        self.lock = threading.Lock()


    def _now_ts(self):
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _log(self, msg):
        self.resource_access_logs.append(msg)
        print(msg, flush=True)


    def _handle_grant(self, process_id, base_priority, work_time, grant_ts):
        """Runs in its own thread for each grant, simulating resource use."""

        time.sleep(work_time)


        with self.lock:
            if process_id in self.active_processes:
                self.active_processes.remove(process_id)


            ct = time.time()
            if process_id in self.completion_times:
                self.completion_times[process_id].append(ct)
            else:
                self.completion_times[process_id] = [ct]

            ts = self._now_ts()
            self._log(f"{ts} - Process {process_id} completed task, held resource for {work_time:.2f} seconds")

    def allocate_resources(self):
        """Fair allocator with aging and true parallel grants."""
        self._log("Resource allocator started")


        while not self.exit_flag.is_set() or not self.process_queue.empty() or self.wait_list or self.active_processes:

            try:
                while True:
                    pid, prio, wtime = self.process_queue.get_nowait()
                    with self.lock:
                        self.wait_list.append((pid, prio, wtime, time.time()))
            except queue.Empty:
                pass


            with self.lock:
                if self.wait_list:
                    now = time.time()


                    enriched = []
                    for idx, (pid, base_prio, wtime, enq_ts) in enumerate(self.wait_list):
                        waited = max(0.0, now - enq_ts)
                        eff = base_prio + self.aging_rate * waited
                        enriched.append((eff, -enq_ts, pid, base_prio, wtime, enq_ts))


                    enriched.sort(reverse=True)


                    grants = []
                    free_units = self.resource_units - len(self.active_processes)
                    for item in enriched:
                        if free_units <= 0:
                            break
                        _, _, pid, base_prio, wtime, enq_ts = item
                        if pid in self.active_processes:
                            continue
                        grants.append((pid, base_prio, wtime, enq_ts))
                        free_units -= 1


                    for pid, base_prio, wtime, enq_ts in grants:

                        for i, (p, bp, wt, ts_enq) in enumerate(self.wait_list):
                            if p == pid and bp == base_prio and abs(wt - wtime) < 1e-9 and ts_enq == enq_ts:
                                self.wait_list.pop(i)
                                break

                        self.active_processes.add(pid)

                        wait_time = now - self.process_start_times.get(pid, enq_ts)
                        if pid in self.waiting_times:
                            self.waiting_times[pid].append(wait_time)
                        else:
                            self.waiting_times[pid] = [wait_time]

                        ts = self._now_ts()
                        self._log(f"{ts} - Process {pid} (Priority: {base_prio}) granted resource after waiting {wait_time:.2f}s")


                        t = threading.Thread(target=self._handle_grant, args=(pid, base_prio, wtime, now), daemon=True)
                        t.start()


            time.sleep(0.01)

        self._log("Resource allocator stopped")

    def request_resource(self, process_id, priority, work_time):
        """
        Called by worker processes. Enqueues a request.
        """

        if process_id not in self.process_start_times:
            self.process_start_times[process_id] = time.time()

        self.process_queue.put((process_id, priority, work_time))
        ts = self._now_ts()
        self._log(f"{ts} - Process {process_id} (Priority: {priority}) requested resource for {work_time:.2f} seconds")





def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    try:
        for _ in range(work_iterations):

            work_time = random.uniform(0.5, 2.0)
            resource_manager.request_resource(process_id, priority, work_time)


            time.sleep(random.uniform(*delay_range))
    except Exception as e:
        print(f"Error in worker process {process_id}: {e}", flush=True)


def analyze_results(resource_manager):
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)


    avg_wait = {pid: (sum(v) / len(v) if v else 0.0) for pid, v in waiting_times.items()}


    tasks = {pid: len(v) for pid, v in completion_times.items()}

    print("\nProcess Summary")
    print("---------------")
    for pid in sorted(avg_wait.keys()):
        print(f"Process {pid}: Avg Wait: {avg_wait[pid]:.2f}s, Tasks Completed: {tasks.get(pid,0)}")


    starved = [pid for pid, w in avg_wait.items() if w > 2.0]
    if starved:
        print("\nStarved Processes (avg wait > 2s):")
        for pid in starved:
            print(f"  - Process {pid}: Avg Wait {avg_wait[pid]:.2f}s")


    try:
        import matplotlib.pyplot as plt

        pids = sorted(avg_wait.keys())
        waits = [avg_wait[pid] for pid in pids]
        done = [tasks.get(pid, 0) for pid in pids]

        plt.figure(figsize=(8, 4))
        plt.bar(pids, waits)
        plt.title('Average Waiting Time by Process')
        plt.xlabel('Process ID'); plt.ylabel('Avg Wait (s)')
        plt.grid(axis='y', linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.savefig('waiting_times.png')

        plt.figure(figsize=(8, 4))
        plt.bar(pids, done)
        plt.title('Tasks Completed by Process')
        plt.xlabel('Process ID'); plt.ylabel('Tasks')
        plt.grid(axis='y', linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.savefig('tasks_completed.png')

        print("\nSaved plots: waiting_times.png, tasks_completed.png")
    except Exception as e:
        print(f"Plotting skipped: {e}")


    with open('resource_access_logs.txt', 'w') as f:
        for line in resource_manager.resource_access_logs:
            f.write(line + '\n')
    print("Detailed logs saved to resource_access_logs.txt\n")


def main():
    num_processes = 5
    resource_units = 2
    simulation_duration = 30


    resource_manager = ResourceManager(resource_units=resource_units, aging_rate=0.8)

    allocator_thread = threading.Thread(target=resource_manager.allocate_resources, daemon=True)
    allocator_thread.start()


    procs = []
    for i in range(2):
        p = mp.Process(target=worker_process, args=(i, 9, 10, resource_manager, (0.5, 1.5)))
        procs.append(p)
    for i in range(2, 4):
        p = mp.Process(target=worker_process, args=(i, 5, 10, resource_manager, (0.5, 1.5)))
        procs.append(p)
    p = mp.Process(target=worker_process, args=(4, 1, 10, resource_manager, (0.5, 1.5)))
    procs.append(p)

    print(f"Starting simulation with {num_processes} processes...")
    for p in procs:
        p.start()

    time.sleep(simulation_duration)


    resource_manager.exit_flag.set()
    for p in procs:
        p.join(timeout=3)
        if p.is_alive():
            p.terminate()


    allocator_thread.join(timeout=5)

    print("All processes finished or were terminated.")
    analyze_results(resource_manager)


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()

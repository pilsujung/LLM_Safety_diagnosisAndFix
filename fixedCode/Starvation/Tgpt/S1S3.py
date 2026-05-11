import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import threading
import matplotlib.pyplot as plt





class ResourceManager:
    def __init__(self, resource_units=1, aging_rate=0.25):
        """
        Args:
            resource_units: number of concurrent resources available
            aging_rate: how fast waiting requests gain priority per second
        """
        self.resource_units = resource_units
        self.aging_rate = aging_rate


        self.manager = mp.Manager()
        self.completion_times = self.manager.dict()
        self.waiting_times = self.manager.dict()
        self.resource_access_logs = self.manager.list()
        self.process_start_times = self.manager.dict()


        self.process_queue = mp.Queue()


        self._waiting = []
        self._active_count = 0
        self._seq = 0
        self._lock = threading.Lock()

        self.exit_flag = mp.Event()

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        full = f"{ts} - {msg}"
        self.resource_access_logs.append(full)
        print(full)

    def allocate_resources(self):
        """Starvation-free allocator with aging and non-blocking dispatch."""
        print("Resource allocator started")

        while not self.exit_flag.is_set():
            now = time.time()


            drained = 0
            while True:
                try:
                    pid, base_pri, work_time, req_seq = self.process_queue.get_nowait()
                    with self._lock:
                        self._waiting.append({
                            "pid": pid,
                            "base": base_pri,
                            "work": work_time,
                            "enqueued": now,
                            "seq": req_seq
                        })
                    drained += 1
                except queue.Empty:
                    break


            dispatched = 0
            while True:
                with self._lock:
                    if self._active_count >= self.resource_units or not self._waiting:
                        break


                    now2 = time.time()
                    for item in self._waiting:
                        waited = max(0.0, now2 - item["enqueued"])
                        item["eff"] = item["base"] + self.aging_rate * waited


                    self._waiting.sort(key=lambda x: (-x["eff"], x["enqueued"], x["seq"]))
                    job = self._waiting.pop(0)
                    self._active_count += 1


                wait_time = time.time() - job["enqueued"]
                pid = job["pid"]
                base = job["base"]
                work = job["work"]


                if pid in self.waiting_times:
                    self.waiting_times[pid].append(wait_time)
                else:
                    self.waiting_times[pid] = [wait_time]

                self._log(f"Process {pid} (base priority {base}, eff {job['eff']:.2f}) "
                          f"granted resource after waiting {wait_time:.2f}s for {work:.2f}s")


                t = threading.Thread(target=self._serve_and_release, args=(pid, base, work))
                t.daemon = True
                t.start()
                dispatched += 1


            time.sleep(0.01 if (drained or dispatched) else 0.05)

        print("Resource allocator stopped")

    def _serve_and_release(self, pid, base, work_time):
        """Simulate resource usage and release without blocking the allocator loop."""
        try:
            time.sleep(work_time)
        finally:

            if pid in self.completion_times:
                self.completion_times[pid].append(time.time())
            else:
                self.completion_times[pid] = [time.time()]

            self._log(f"Process {pid} completed task, held resource for {work_time:.2f}s")

            with self._lock:
                self._active_count -= 1

    def request_resource(self, process_id, priority, work_time):
        """
        Called by worker processes to enqueue a resource request (non-blocking).
        """

        self.process_start_times[process_id] = time.time()


        with self._lock:
            self._seq += 1
            seq = self._seq

        self.process_queue.put((process_id, priority, work_time, seq))
        self._log(f"Process {process_id} (Priority: {priority}) requested resource access for {work_time:.2f}s")





def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    try:
        for _ in range(work_iterations):
            work_time = random.uniform(0.5, 2.0)
            resource_manager.request_resource(process_id, priority, work_time)
            time.sleep(random.uniform(delay_range[0], delay_range[1]))
    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")

def analyze_results(resource_manager):
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)


    avg_waiting_times = {pid: (sum(w)/len(w)) for pid, w in waiting_times.items() if w}


    tasks_completed = {pid: len(c) for pid, c in completion_times.items()}

    print("\nProcess Summary:\n---------------")
    for pid in sorted(avg_waiting_times.keys()):
        tc = tasks_completed.get(pid, 0)
        print(f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s, Tasks Completed: {tc}")


    starved_threshold = 2.0
    starved = [pid for pid, wt in avg_waiting_times.items() if wt > starved_threshold]

    if starved:
        print("\nStarved Processes:\n----------------")
        for pid in starved:
            print(f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s")


    if avg_waiting_times:
        process_ids = sorted(avg_waiting_times.keys())
        avg_waits = [avg_waiting_times[p] for p in process_ids]
        plt.figure(figsize=(10, 6))
        plt.bar(process_ids, avg_waits, color=['red' if p in starved else 'blue' for p in process_ids])
        plt.title('Average Waiting Time by Process')
        plt.xlabel('Process ID')
        plt.ylabel('Average Waiting Time (seconds)')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.savefig('waiting_times.png')

        tasks = [tasks_completed.get(p, 0) for p in process_ids]
        plt.figure(figsize=(10, 6))
        plt.bar(process_ids, tasks, color=['green' if tasks_completed.get(p, 0) > 1 else 'orange' for p in process_ids])
        plt.title('Tasks Completed by Process')
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
    resource_units = 1
    simulation_duration = 30

    resource_manager = ResourceManager(resource_units=resource_units, aging_rate=0.35)

    allocator_thread = threading.Thread(target=resource_manager.allocate_resources, daemon=True)
    allocator_thread.start()

    processes = []


    for i in range(2):
        p = mp.Process(target=worker_process, args=(i, 9, 10, resource_manager, (0.5, 1.5)))
        processes.append(p)


    for i in range(2, 4):
        p = mp.Process(target=worker_process, args=(i, 5, 10, resource_manager, (0.5, 1.5)))
        processes.append(p)


    p = mp.Process(target=worker_process, args=(4, 1, 10, resource_manager, (0.5, 1.5)))
    processes.append(p)

    print(f"Starting simulation with {num_processes} processes...")
    for p in processes:
        p.start()


    time.sleep(simulation_duration)


    resource_manager.exit_flag.set()
    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    print("All processes have completed or been terminated")
    analyze_results(resource_manager)

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()

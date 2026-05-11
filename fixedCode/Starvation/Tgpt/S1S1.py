import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import threading


class ResourceManager:
    def __init__(self, resource_units=1, aging_rate=0.5, starvation_threshold=3.0):
        """
        Initialize the resource manager with fairness controls.

        Args:
            resource_units: Number of concurrent resources available.
            aging_rate: How fast waiting increases effective priority (priority points per second).
            starvation_threshold: Force-serve the oldest request once waited this many seconds.
        """
        self.resource_units = resource_units
        self.resource_lock = mp.Lock()
        self.process_queue = mp.Queue()


        self.pending = []
        self.active_processes = set()

        self.completion_times = mp.Manager().dict()
        self.waiting_times = mp.Manager().dict()
        self.resource_access_logs = mp.Manager().list()
        self.process_start_times = mp.Manager().dict()
        self.exit_flag = mp.Event()

        self.aging_rate = aging_rate
        self.starvation_threshold = starvation_threshold

    def allocate_resources(self):
        """Fair resource allocator with aging and starvation prevention."""
        print("Resource allocator started")

        while not self.exit_flag.is_set():
            try:

                while True:
                    try:
                        process_id, priority, work_time = self.process_queue.get(block=False)
                        self.pending.append({
                            "process_id": process_id,
                            "base_priority": priority,
                            "work_time": work_time,
                            "arrival_time": time.time()
                        })
                    except queue.Empty:
                        break


                if not self.pending or len(self.active_processes) >= self.resource_units:
                    time.sleep(0.05)
                    continue


                now = time.time()


                oldest_idx = min(range(len(self.pending)),
                                 key=lambda i: self.pending[i]["arrival_time"])
                oldest_wait = now - self.pending[oldest_idx]["arrival_time"]

                if oldest_wait >= self.starvation_threshold:
                    chosen_idx = oldest_idx
                else:

                    def eff_prio(item):
                        waited = now - item["arrival_time"]
                        return item["base_priority"] + self.aging_rate * waited

                    chosen_idx = max(range(len(self.pending)),
                                     key=lambda i: eff_prio(self.pending[i]))

                job = self.pending.pop(chosen_idx)
                pid = job["process_id"]
                prio = job["base_priority"]
                work_time = job["work_time"]


                self.active_processes.add(pid)


                wait_time = now - self.process_start_times[pid]
                if pid in self.waiting_times:
                    self.waiting_times[pid].append(wait_time)
                else:
                    self.waiting_times[pid] = [wait_time]


                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                log_entry = (f"{timestamp} - Process {pid} (Priority: {prio}) "
                             f"granted resource after waiting {wait_time:.2f}s")
                self.resource_access_logs.append(log_entry)
                print(log_entry)


                time.sleep(work_time)


                self.active_processes.remove(pid)


                completion_time = time.time()
                if pid in self.completion_times:
                    self.completion_times[pid].append(completion_time)
                else:
                    self.completion_times[pid] = [completion_time]

                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                log_entry = (f"{timestamp} - Process {pid} completed task, "
                             f"held resource for {work_time:.2f}s")
                self.resource_access_logs.append(log_entry)
                print(log_entry)

                time.sleep(0.05)

            except Exception as e:
                print(f"Error in allocator: {e}")

        print("Resource allocator stopped")

    def request_resource(self, process_id, priority, work_time):
        """Submit a resource request."""
        self.process_start_times[process_id] = time.time()
        self.process_queue.put((process_id, priority, work_time))
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = (f"{timestamp} - Process {process_id} (Priority: {priority}) "
                     f"requested resource for {work_time:.2f}s")
        self.resource_access_logs.append(log_entry)
        print(log_entry)


def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    """Worker process repeatedly requesting access."""
    try:
        for _ in range(work_iterations):
            work_time = random.uniform(0.5, 2.0)
            resource_manager.request_resource(process_id, priority, work_time)
            delay = random.uniform(delay_range[0], delay_range[1])
            time.sleep(delay)
    except Exception as e:
        print(f"Error in worker {process_id}: {e}")


def analyze_results(resource_manager):
    """Compute and display performance metrics."""
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)

    avg_waiting = {pid: sum(w) / len(w) for pid, w in waiting_times.items()}
    tasks_completed = {pid: len(c) for pid, c in completion_times.items()}

    print("\nProcess Summary:")
    for pid in sorted(avg_waiting.keys()):
        print(f"Process {pid}: Avg Wait {avg_waiting[pid]:.2f}s, "
              f"Tasks Completed {tasks_completed[pid]}")

    starved = [pid for pid, w in avg_waiting.items() if w > 2.0]
    if starved:
        print("\nStarved Processes:")
        for pid in starved:
            print(f"  Process {pid} Avg Wait {avg_waiting[pid]:.2f}s")


    pids = sorted(avg_waiting.keys())
    waits = [avg_waiting[p] for p in pids]
    plt.figure(figsize=(10, 6))
    plt.bar(pids, waits,
            color=['red' if p in starved else 'blue' for p in pids])
    plt.title("Average Waiting Time per Process")
    plt.xlabel("Process ID")
    plt.ylabel("Avg Waiting Time (s)")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig("waiting_times.png")

    tasks = [tasks_completed[p] for p in pids]
    plt.figure(figsize=(10, 6))
    plt.bar(pids, tasks,
            color=['green' if tasks_completed[p] > 1 else 'orange' for p in pids])
    plt.title("Tasks Completed per Process")
    plt.xlabel("Process ID")
    plt.ylabel("Tasks Completed")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig("tasks_completed.png")

    with open("resource_access_logs.txt", "w") as f:
        for log in resource_manager.resource_access_logs:
            f.write(log + "\n")

    print("\nSaved: waiting_times.png, tasks_completed.png, resource_access_logs.txt")


def main():
    num_processes = 5
    resource_units = 1
    simulation_duration = 30

    resource_manager = ResourceManager(resource_units=resource_units)

    allocator_thread = threading.Thread(target=resource_manager.allocate_resources)
    allocator_thread.daemon = True
    allocator_thread.start()

    processes = []

    for i in range(2):
        p = mp.Process(target=worker_process,
                       args=(i, 9, 10, resource_manager, (0.5, 1.5)))
        processes.append(p)

    for i in range(2, 4):
        p = mp.Process(target=worker_process,
                       args=(i, 5, 10, resource_manager, (0.5, 1.5)))
        processes.append(p)

    p = mp.Process(target=worker_process,
                   args=(4, 1, 10, resource_manager, (0.5, 1.5)))
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

    print("All processes completed or terminated")
    analyze_results(resource_manager)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()

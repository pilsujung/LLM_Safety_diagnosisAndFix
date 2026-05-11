import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import matplotlib.pyplot as plt
import threading


class ResourceManager:
    """
    Central coordinator:
    - Receives resource requests from worker processes
    - Allocates limited resource_units fairly (priority + aging)
    - Records wait/completion stats for analysis
    """

    def __init__(
        self,
        resource_units,
        request_queue,
        waiting_times,
        completion_times,
        access_logs,
        process_start_times,
        exit_flag,
        aging_rate=0.1,
    ):
        """
        All arguments here are already Manager-backed or mp.* primitives so they
        are safe to share across processes created with spawn.

        resource_units: how many concurrent "slots" we can give
        request_queue: mp.Queue() where workers push (process_id, base_priority, work_time, enqueue_ts)
        waiting_times: Manager.dict()  pid -> list[wait_durations]
        completion_times: Manager.dict() pid -> list[completion_timestamps]
        access_logs: Manager.list()     list[str] timeline log
        process_start_times: Manager.dict() pid -> enqueue_ts for current request
        exit_flag: mp.Event() stop signal
        aging_rate: how fast low-priority jobs age upward toward fairness
        """
        self.resource_units = resource_units
        self.request_queue = request_queue

        self.waiting_times = waiting_times
        self.completion_times = completion_times
        self.access_logs = access_logs
        self.process_start_times = process_start_times

        self.exit_flag = exit_flag


        self.active_processes = set()
        self.wait_list = []
        self.aging_rate = aging_rate


        self.local_lock = threading.Lock()

    def log(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        full = f"{timestamp} - {msg}"
        self.access_logs.append(full)
        print(full, flush=True)

    def _drain_new_requests(self):
        """
        Pull all newly arrived requests from the cross-process mp.Queue()
        into our local wait_list.
        Each item is a dict so we can store enqueue time for aging.
        """
        while True:
            try:
                process_id, base_priority, work_time, enqueue_ts = self.request_queue.get_nowait()
            except queue.Empty:
                break
            job = {
                "pid": process_id,
                "base_priority": base_priority,
                "work_time": work_time,
                "enqueue_ts": enqueue_ts,
            }
            self.wait_list.append(job)

            self.process_start_times[process_id] = enqueue_ts
            self.log(
                f"Process {process_id} (Priority: {base_priority}) requested resource access for {work_time:.2f} seconds"
            )

    def _pick_next_job(self):
        """
        Choose next job to run using PRIORITY + AGING.
        Returns index in self.wait_list.
        """
        if not self.wait_list:
            return None

        now = time.time()



        scored = []
        for idx, job in enumerate(self.wait_list):
            wait_seconds = now - job["enqueue_ts"]
            eff_prio = job["base_priority"] + self.aging_rate * wait_seconds
            scored.append(
                (
                    eff_prio,
                    wait_seconds,
                    -job["enqueue_ts"],
                    idx,
                )
            )



        scored.sort(reverse=True)
        chosen_idx = scored[0][3]
        return chosen_idx

    def allocate_resources(self):
        """
        Runs in a dedicated *thread* in the main process.
        Simulates the resource being granted to at most resource_units jobs at once.
        For simplicity we run them sequentially inside this loop:
          - pick 1 job
          - "run" it by sleeping work_time
          - record stats
        """
        self.log("Resource allocator started")

        while not self.exit_flag.is_set():
            with self.local_lock:

                self._drain_new_requests()


                if len(self.active_processes) < self.resource_units and self.wait_list:
                    pick_idx = self._pick_next_job()
                    if pick_idx is not None:
                        job = self.wait_list.pop(pick_idx)
                        pid = job["pid"]
                        base_priority = job["base_priority"]
                        work_time = job["work_time"]
                        enqueue_ts = job["enqueue_ts"]

                        self.active_processes.add(pid)


                        wait_time = time.time() - enqueue_ts
                        if pid in self.waiting_times:
                            self.waiting_times[pid].append(wait_time)
                        else:
                            self.waiting_times[pid] = [wait_time]

                        self.log(
                            f"Process {pid} (Priority: {base_priority}) granted resource access after waiting {wait_time:.2f} seconds"
                        )


                running_pid = None
                if self.active_processes:


                    running_pid = next(iter(self.active_processes))


            if running_pid is not None:




                pass


            time.sleep(0.05)

        self.log("Resource allocator stopped")






class ResourceManager:
    def __init__(
        self,
        resource_units,
        request_queue,
        waiting_times,
        completion_times,
        access_logs,
        process_start_times,
        exit_flag,
        aging_rate=0.1,
    ):
        self.resource_units = resource_units
        self.request_queue = request_queue

        self.waiting_times = waiting_times
        self.completion_times = completion_times
        self.access_logs = access_logs
        self.process_start_times = process_start_times

        self.exit_flag = exit_flag

        self.aging_rate = aging_rate

        self.local_lock = threading.Lock()
        self.wait_list = []
        self.active_slots = 0

    def log(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        full = f"{timestamp} - {msg}"
        self.access_logs.append(full)
        print(full, flush=True)

    def _drain_new_requests(self):
        while True:
            try:
                process_id, base_priority, work_time, enqueue_ts = self.request_queue.get_nowait()
            except queue.Empty:
                break
            self.wait_list.append(
                {
                    "pid": process_id,
                    "base_priority": base_priority,
                    "work_time": work_time,
                    "enqueue_ts": enqueue_ts,
                }
            )
            self.process_start_times[process_id] = enqueue_ts
            self.log(
                f"Process {process_id} (Priority: {base_priority}) requested resource access for {work_time:.2f} seconds"
            )

    def _pick_next_job(self):
        if not self.wait_list:
            return None

        now = time.time()
        scored = []
        for idx, job in enumerate(self.wait_list):
            wait_seconds = now - job["enqueue_ts"]
            eff_prio = job["base_priority"] + self.aging_rate * wait_seconds

            scored.append((eff_prio, wait_seconds, -job["enqueue_ts"], idx))

        scored.sort(reverse=True)
        return scored[0][3]

    def allocate_resources(self):
        self.log("Resource allocator started")

        while not self.exit_flag.is_set():

            with self.local_lock:
                self._drain_new_requests()


                while self.active_slots < self.resource_units and self.wait_list:
                    chosen_idx = self._pick_next_job()
                    job = self.wait_list.pop(chosen_idx)

                    pid = job["pid"]
                    base_priority = job["base_priority"]
                    work_time = job["work_time"]
                    enqueue_ts = job["enqueue_ts"]

                    wait_time = time.time() - enqueue_ts
                    if pid in self.waiting_times:
                        self.waiting_times[pid].append(wait_time)
                    else:
                        self.waiting_times[pid] = [wait_time]

                    self.log(
                        f"Process {pid} (Priority: {base_priority}) granted resource access after waiting {wait_time:.2f} seconds"
                    )


                    self.active_slots += 1





                    run_start = time.time()
                    time.sleep(work_time)
                    run_end = time.time()


                    if pid in self.completion_times:
                        self.completion_times[pid].append(run_end)
                    else:
                        self.completion_times[pid] = [run_end]

                    self.log(
                        f"Process {pid} completed task, held resource for {work_time:.2f} seconds"
                    )

                    self.active_slots -= 1


            time.sleep(0.05)

        self.log("Resource allocator stopped")


def worker_process(process_id, priority, work_iterations, request_queue, exit_flag, delay_range):
    """
    Each worker just enqueues requests. The allocator thread will consume them.
    """
    try:
        for _ in range(work_iterations):
            if exit_flag.is_set():
                break

            work_time = random.uniform(0.5, 2.0)
            enqueue_ts = time.time()


            request_queue.put((process_id, priority, work_time, enqueue_ts))


            delay = random.uniform(delay_range[0], delay_range[1])
            time.sleep(delay)

    except Exception as e:
        print(f"Error in worker process {process_id}: {e}", flush=True)


def analyze_results(waiting_times, completion_times, access_logs):
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(waiting_times)
    completion_times = dict(completion_times)


    avg_waiting_times = {}
    for pid, waits in waiting_times.items():
        avg_waiting_times[pid] = sum(waits) / len(waits) if waits else 0.0


    tasks_completed = {pid: len(comps) for pid, comps in completion_times.items()}

    print("\nProcess Summary:")
    print("---------------")
    for pid in sorted(avg_waiting_times.keys()):
        print(
            f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s, "
            f"Tasks Completed: {tasks_completed.get(pid, 0)}"
        )


    starved_threshold = 2.0
    starved = [
        pid
        for pid, avg_w in avg_waiting_times.items()
        if avg_w > starved_threshold or tasks_completed.get(pid, 0) == 0
    ]

    if starved:
        print("\nStarved (or nearly starved) Processes:")
        print("--------------------------------------")
        for pid in starved:
            print(
                f"Process {pid}: Avg Wait {avg_waiting_times[pid]:.2f}s, "
                f"Tasks {tasks_completed.get(pid, 0)}"
            )
    else:
        print("\nNo starvation detected 🎉")


    pids_sorted = sorted(avg_waiting_times.keys())
    avg_waits_sorted = [avg_waiting_times[pid] for pid in pids_sorted]
    colors_wait = [
        "red" if pid in starved else "blue" for pid in pids_sorted
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(pids_sorted, avg_waits_sorted, color=colors_wait)
    plt.title("Average Waiting Time by Process (with aging)")
    plt.xlabel("Process ID")
    plt.ylabel("Average Waiting Time (seconds)")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig("waiting_times.png")


    tasks_sorted = [tasks_completed.get(pid, 0) for pid in pids_sorted]
    colors_tasks = [
        "green" if tasks_completed.get(pid, 0) > 1 else "orange" for pid in pids_sorted
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(pids_sorted, tasks_sorted, color=colors_tasks)
    plt.title("Tasks Completed by Process (with aging)")
    plt.xlabel("Process ID")
    plt.ylabel("Number of Tasks Completed")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig("tasks_completed.png")

    print("\nSaved visualizations: waiting_times.png and tasks_completed.png")

    with open("resource_access_logs.txt", "w") as f:
        for line in access_logs:
            f.write(line + "\n")

    print("\nDetailed logs saved to resource_access_logs.txt")


def main():

    num_processes = 5
    resource_units = 1
    simulation_duration = 30
    work_iterations = 10
    delay_range = (0.5, 1.5)


    manager = mp.Manager()

    request_queue = mp.Queue()
    waiting_times = manager.dict()
    completion_times = manager.dict()
    access_logs = manager.list()
    process_start_times = manager.dict()
    exit_flag = mp.Event()


    rm = ResourceManager(
        resource_units=resource_units,
        request_queue=request_queue,
        waiting_times=waiting_times,
        completion_times=completion_times,
        access_logs=access_logs,
        process_start_times=process_start_times,
        exit_flag=exit_flag,
        aging_rate=0.1,
    )

    allocator_thread = threading.Thread(target=rm.allocate_resources, daemon=True)
    allocator_thread.start()


    processes = []

    for pid in range(2):
        p = mp.Process(
            target=worker_process,
            args=(pid, 9, work_iterations, request_queue, exit_flag, delay_range),
        )
        processes.append(p)


    for pid in range(2, 4):
        p = mp.Process(
            target=worker_process,
            args=(pid, 5, work_iterations, request_queue, exit_flag, delay_range),
        )
        processes.append(p)


    low_pid = 4
    p = mp.Process(
        target=worker_process,
        args=(low_pid, 1, work_iterations, request_queue, exit_flag, delay_range),
    )
    processes.append(p)

    print(f"Starting simulation with {num_processes} processes...")

    for p in processes:
        p.start()


    time.sleep(simulation_duration)


    exit_flag.set()


    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()



    time.sleep(0.2)

    print("All processes have completed or been terminated")


    analyze_results(waiting_times, completion_times, access_logs)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()

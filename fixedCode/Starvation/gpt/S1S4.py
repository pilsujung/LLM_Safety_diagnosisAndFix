import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import matplotlib.pyplot as plt
import threading
import math


def now_ts():
    return time.time()


class ResourceManager:
    def __init__(self, shared_state, resource_units=1, aging_factor=0.5):
        """
        resource_units: how many parallel "slots" the resource has
        aging_factor: how fast waiting boosts priority per second
        """
        self.resource_units = resource_units
        self.aging_factor = aging_factor


        self.request_queue = shared_state["request_queue"]
        self.waiting_times = shared_state["waiting_times"]
        self.completion_times = shared_state["completion_times"]
        self.resource_access_logs = shared_state["logs"]


        self.active_processes = set()
        self.pending_requests = []
        self.exit_flag = shared_state["exit_flag"]

    def _log(self, msg):
        print(msg)
        self.resource_access_logs.append(msg)

    def allocate_resources(self):
        """
        Fair resource allocator with AGING:
        effective_priority = base_priority + aging_factor * wait_seconds
        The oldest low-priority request will eventually outrank spammy high-priority ones.
        """
        self._log("Resource allocator started")

        while not self.exit_flag.is_set():

            while True:
                try:
                    req = self.request_queue.get_nowait()



                    self.pending_requests.append(req)
                except queue.Empty:
                    break


            while len(self.active_processes) < self.resource_units and self.pending_requests:

                current_time = now_ts()
                for r in self.pending_requests:
                    wait_sec = current_time - r["enqueue_time"]
                    r["effective_priority"] = r["base_priority"] + self.aging_factor * wait_sec


                self.pending_requests.sort(
                    key=lambda r: (r["effective_priority"],
                                   current_time - r["enqueue_time"]),
                    reverse=True
                )
                req = self.pending_requests.pop(0)

                pid = req["process_id"]
                base_prio = req["base_priority"]
                work_time = req["work_time"]
                waited = current_time - req["enqueue_time"]


                self.active_processes.add(pid)


                if pid not in self.waiting_times:
                    self.waiting_times[pid] = [waited]
                else:
                    self.waiting_times[pid].append(waited)


                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                self._log(
                    f"{ts} - Process {pid} (BasePrio: {base_prio:.0f}, "
                    f"EffPrio: {req['effective_priority']:.2f}) "
                    f"granted resource after waiting {waited:.2f}s "
                    f"for {work_time:.2f}s"
                )



                time.sleep(work_time)


                self.active_processes.remove(pid)

                done_t = now_ts()
                if pid not in self.completion_times:
                    self.completion_times[pid] = [done_t]
                else:
                    self.completion_times[pid].append(done_t)

                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                self._log(
                    f"{ts} - Process {pid} completed task, "
                    f"held resource for {work_time:.2f}s"
                )


            time.sleep(0.05)

        self._log("Resource allocator stopped")


def worker_process(
    process_id,
    base_priority,
    work_iterations,
    request_queue,
    delay_range
):
    """
    A worker just posts requests into the shared request_queue.
    The allocator thread (in main proc) will schedule them fairly.
    """
    try:
        rng = random.Random(process_id ^ int(now_ts()))
        for _ in range(work_iterations):
            work_time = rng.uniform(0.5, 2.0)

            enqueue_time = now_ts()
            request_queue.put({
                "process_id": process_id,
                "base_priority": base_priority,
                "work_time": work_time,
                "enqueue_time": enqueue_time,
            })

            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(
                f"{ts} - Process {process_id} (BasePrio: {base_priority}) "
                f"requested resource for {work_time:.2f}s"
            )


            delay = rng.uniform(delay_range[0], delay_range[1])
            time.sleep(delay)

    except Exception as e:
        print(f"Error in worker {process_id}: {e}")


def analyze_results(waiting_times, completion_times, logs):
    print("\n=== SIMULATION RESULTS ===")

    waiting_times_local = dict(waiting_times)
    completion_times_local = dict(completion_times)


    avg_wait = {
        pid: (sum(wts) / len(wts) if wts else math.nan)
        for pid, wts in waiting_times_local.items()
    }


    tasks_done = {
        pid: len(completion_times_local.get(pid, []))
        for pid in waiting_times_local.keys() | completion_times_local.keys()
    }

    print("\nProcess Summary:")
    print("---------------")
    for pid in sorted(tasks_done.keys()):
        aw = avg_wait.get(pid, float('nan'))
        td = tasks_done[pid]
        print(f"Process {pid}: Avg Wait {aw:.2f}s, Tasks {td}")


    STARVED_THRESHOLD = 2.0
    starved = [
        pid for pid, w in avg_wait.items()
        if not math.isnan(w) and w > STARVED_THRESHOLD
    ]

    if starved:
        print("\nStarved (avg wait > 2.0s):")
        print("--------------------------")
        for pid in starved:
            print(f"Process {pid}: Avg Wait {avg_wait[pid]:.2f}s")
    else:
        print("\nNo starvation detected 🎉")


    pids_sorted = sorted(tasks_done.keys())

    waits_bar = [avg_wait[pid] for pid in pids_sorted]
    colors_wait = [
        "red" if (pid in starved) else "blue"
        for pid in pids_sorted
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(pids_sorted, waits_bar, color=colors_wait)
    plt.title("Average Waiting Time by Process (with aging)")
    plt.xlabel("Process ID")
    plt.ylabel("Average Waiting Time (seconds)")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig("waiting_times.png")

    task_bar = [tasks_done[pid] for pid in pids_sorted]
    colors_task = [
        "green" if tasks_done[pid] > 1 else "orange"
        for pid in pids_sorted
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(pids_sorted, task_bar, color=colors_task)
    plt.title("Tasks Completed by Process")
    plt.xlabel("Process ID")
    plt.ylabel("Number of Tasks Completed")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig("tasks_completed.png")

    print("\nSaved visualizations: waiting_times.png and tasks_completed.png")

    with open("resource_access_logs.txt", "w") as f:
        for line in logs:
            f.write(line + "\n")

    print("Detailed logs saved to resource_access_logs.txt\n")


def main():

    num_processes = 5
    resource_units = 1
    simulation_duration = 30
    work_iterations = 10
    aging_factor = 0.5


    mgr = mp.Manager()
    shared_state = {
        "request_queue": mp.Queue(),
        "waiting_times": mgr.dict(),
        "completion_times": mgr.dict(),
        "logs": mgr.list(),
        "exit_flag": mp.Event(),
    }


    resource_manager = ResourceManager(
        shared_state=shared_state,
        resource_units=resource_units,
        aging_factor=aging_factor,
    )

    allocator_thread = threading.Thread(
        target=resource_manager.allocate_resources,
        daemon=True
    )
    allocator_thread.start()


    processes = []


    processes.append(mp.Process(
        target=worker_process,
        args=(0, 9, work_iterations, shared_state["request_queue"], (0.5, 1.5))
    ))
    processes.append(mp.Process(
        target=worker_process,
        args=(1, 9, work_iterations, shared_state["request_queue"], (0.5, 1.5))
    ))


    processes.append(mp.Process(
        target=worker_process,
        args=(2, 5, work_iterations, shared_state["request_queue"], (0.5, 1.5))
    ))
    processes.append(mp.Process(
        target=worker_process,
        args=(3, 5, work_iterations, shared_state["request_queue"], (0.5, 1.5))
    ))


    processes.append(mp.Process(
        target=worker_process,
        args=(4, 1, work_iterations, shared_state["request_queue"], (0.5, 1.5))
    ))

    print(f"Starting simulation with {num_processes} processes...")

    for p in processes:
        p.start()


    time.sleep(simulation_duration)


    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    print("All worker processes have completed or been terminated")


    shared_state["exit_flag"].set()
    allocator_thread.join(timeout=2)


    analyze_results(
        waiting_times=shared_state["waiting_times"],
        completion_times=shared_state["completion_times"],
        logs=shared_state["logs"],
    )


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()

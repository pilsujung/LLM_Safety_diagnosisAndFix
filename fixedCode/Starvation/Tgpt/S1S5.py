import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import threading
import matplotlib.pyplot as plt


class ResourceManager:
    def __init__(self, resource_units=1, aging_factor=0.5):
        """
        Initialize the resource manager with a limited number of resource units.

        Args:
            resource_units: number of concurrent resources available
            aging_factor: how fast waiting boosts effective priority
                          (larger = more fairness/sooner service for low prio)
        """
        self.resource_units = resource_units
        self.resource_lock = mp.Lock()


        self.process_queue = mp.Queue()









        self.priority_queue = []


        self.active_processes = set()

        mgr = mp.Manager()
        self.completion_times = mgr.dict()
        self.waiting_times = mgr.dict()
        self.resource_access_logs = mgr.list()
        self.process_start_times = mgr.dict()
        self.exit_flag = mp.Event()


        self.aging_factor = aging_factor

    def _log(self, msg: str):
        """Helper to log to console and shared list."""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        line = f"{timestamp} - {msg}"
        self.resource_access_logs.append(line)
        print(line)

    def _run_task(self, process_id, base_priority, work_time, start_wait_ts):
        """
        Runs the "resource usage" for a single allocated process.
        This simulates the process holding the resource for work_time seconds.
        This function runs in its own thread so allocator can keep scheduling.
        """
        try:

            wait_time = time.time() - start_wait_ts
            if process_id in self.waiting_times:
                self.waiting_times[process_id].append(wait_time)
            else:
                self.waiting_times[process_id] = [wait_time]

            self._log(
                f"Process {process_id} (Priority: {base_priority}) "
                f"granted resource access after waiting {wait_time:.2f} seconds"
            )


            time.sleep(work_time)


            completion_time = time.time()
            if process_id in self.completion_times:
                self.completion_times[process_id].append(completion_time)
            else:
                self.completion_times[process_id] = [completion_time]

            self._log(
                f"Process {process_id} completed task, "
                f"held resource for {work_time:.2f} seconds"
            )

        finally:

            self.active_processes.remove(process_id)

    def allocate_resources(self):
        """
        Starvation-safe resource allocator.
        Uses aging so long-waiting, low-priority processes eventually get served.
        Also yields between allocations instead of hogging.
        """
        print("Resource allocator started")

        while not self.exit_flag.is_set():
            try:

                while True:
                    try:
                        process_id, base_priority, work_time = self.process_queue.get(block=False)
                        enqueue_time = time.time()


                        self.process_start_times[process_id] = enqueue_time

                        self.priority_queue.append({
                            "process_id": process_id,
                            "base_priority": base_priority,
                            "work_time": work_time,
                            "enqueue_time": enqueue_time,
                        })

                        self._log(
                            f"Process {process_id} (Priority: {base_priority}) "
                            f"requested resource access for {work_time:.2f} seconds"
                        )
                    except queue.Empty:
                        break


                scheduled_any = False
                while (
                    len(self.active_processes) < self.resource_units
                    and self.priority_queue
                ):
                    now = time.time()



                    for item in self.priority_queue:
                        wait_seconds = now - item["enqueue_time"]
                        item["effective_priority"] = (
                            item["base_priority"] + self.aging_factor * wait_seconds
                        )


                    self.priority_queue.sort(
                        key=lambda x: x["effective_priority"],
                        reverse=True
                    )
                    chosen = self.priority_queue.pop(0)

                    pid = chosen["process_id"]
                    base_prio = chosen["base_priority"]
                    work_t = chosen["work_time"]
                    start_wait_ts = chosen["enqueue_time"]


                    self.active_processes.add(pid)


                    t = threading.Thread(
                        target=self._run_task,
                        args=(pid, base_prio, work_t, start_wait_ts),
                        daemon=True
                    )
                    t.start()

                    scheduled_any = True



                if not scheduled_any:
                    time.sleep(0.05)

            except Exception as e:
                print(f"Error in resource allocator: {e}")

        print("Resource allocator stopped")

    def request_resource(self, process_id, priority, work_time):
        """
        Worker calls this to ask for the shared resource.
        """

        self.process_queue.put((process_id, priority, work_time))




def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    """
    Worker process that requests resources multiple times.
    """
    try:
        for _ in range(work_iterations):

            work_time = random.uniform(0.5, 2.0)


            resource_manager.request_resource(process_id, priority, work_time)


            time.sleep(random.uniform(delay_range[0], delay_range[1]))

    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")


def analyze_results(resource_manager):
    """Analyze and display the results of the simulation"""
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)


    avg_waiting_times = {}
    for pid, waits in waiting_times.items():
        if waits:
            avg_waiting_times[pid] = sum(waits) / len(waits)
        else:
            avg_waiting_times[pid] = 0.0


    tasks_completed = {}
    for pid, completions in completion_times.items():
        tasks_completed[pid] = len(completions)

    print("\nProcess Summary:")
    print("---------------")
    for pid in sorted(avg_waiting_times.keys()):
        print(
            f"Process {pid}: "
            f"Avg Wait: {avg_waiting_times[pid]:.2f}s, "
            f"Tasks Completed: {tasks_completed.get(pid, 0)}"
        )

    starved_threshold = 2.0
    starved_processes = [
        pid for pid, w in avg_waiting_times.items() if w > starved_threshold
    ]

    if starved_processes:
        print("\nStarved Processes:")
        print("----------------")
        for pid in starved_processes:
            print(f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s")


    process_ids = sorted(avg_waiting_times.keys())
    avg_waits = [avg_waiting_times[pid] for pid in process_ids]

    plt.figure(figsize=(10, 6))
    plt.bar(
        process_ids,
        avg_waits,
        color=["red" if pid in starved_processes else "blue" for pid in process_ids],
    )
    plt.title("Average Waiting Time by Process")
    plt.xlabel("Process ID")
    plt.ylabel("Average Waiting Time (seconds)")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig("waiting_times.png")


    tasks = [tasks_completed.get(pid, 0) for pid in process_ids]

    plt.figure(figsize=(10, 6))
    plt.bar(
        process_ids,
        tasks,
        color=["green" if tasks_completed.get(pid, 0) > 1 else "orange" for pid in process_ids],
    )
    plt.title("Tasks Completed by Process")
    plt.xlabel("Process ID")
    plt.ylabel("Number of Tasks Completed")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig("tasks_completed.png")

    print("\nSaved visualizations: waiting_times.png and tasks_completed.png")


    with open("resource_access_logs.txt", "w") as f:
        for log in resource_manager.resource_access_logs:
            f.write(log + "\n")

    print("\nDetailed logs saved to resource_access_logs.txt")


def main():
    """Main function to run the simulation"""
    num_processes = 5
    resource_units = 1
    simulation_duration = 30

    resource_manager = ResourceManager(
        resource_units=resource_units,
        aging_factor=0.5
    )


    allocator_thread = threading.Thread(
        target=resource_manager.allocate_resources,
        daemon=True
    )
    allocator_thread.start()

    processes = []


    for i in range(2):
        p = mp.Process(
            target=worker_process,
            args=(i, 9, 10, resource_manager, (0.5, 1.5)),
        )
        processes.append(p)


    for i in range(2, 4):
        p = mp.Process(
            target=worker_process,
            args=(i, 5, 10, resource_manager, (0.5, 1.5)),
        )
        processes.append(p)


    p = mp.Process(
        target=worker_process,
        args=(4, 1, 10, resource_manager, (0.5, 1.5)),
    )
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
    mp.set_start_method("spawn", force=True)
    main()

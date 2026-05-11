import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import threading

class ResourceManager:
    def __init__(self, resource_units=1, aging_rate=0.5):
        """
        Initialize the resource manager with a limited number of resource units.

        Args:
            resource_units: number of concurrent resources available
            aging_rate: how fast waiting boosts priority per second
        """
        self.resource_units = resource_units
        self.resource_lock = mp.Lock()


        self.process_queue = mp.Queue()









        self.priority_queue = []

        self.active_processes = set()

        manager = mp.Manager()
        self.completion_times = manager.dict()
        self.waiting_times = manager.dict()
        self.resource_access_logs = manager.list()
        self.process_start_times = manager.dict()
        self.exit_flag = mp.Event()


        self.aging_rate = aging_rate


        self.last_served_process = None

    def _update_internal_queue(self):
        """Drain new requests from the inbound mp.Queue into local list."""
        while not self.process_queue.empty():
            try:
                process_id, priority, work_time, req_time = self.process_queue.get(block=False)
            except queue.Empty:
                break
            self.priority_queue.append({
                "process_id": process_id,
                "base_priority": priority,
                "work_time": work_time,
                "arrival_time": req_time,
            })

    def _choose_next_request_fair(self):
        """
        Pick the next request using priority + aging.
        Also gently discourage picking the exact same process twice in a row
        if there are other viable contenders.
        """
        if not self.priority_queue:
            return None

        now = time.time()


        scored = []
        for req in self.priority_queue:
            wait_sec = now - req["arrival_time"]
            effective_priority = req["base_priority"] + self.aging_rate * wait_sec
            scored.append((effective_priority, wait_sec, req))


        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)




        top_eff, top_wait, top_req = scored[0]

        if len(scored) > 1 and self.last_served_process == top_req["process_id"]:

            for eff, wt, req in scored[1:]:

                if eff >= top_eff - 0.25:
                    return req

        return top_req

    def allocate_resources(self):
        """Fair resource allocation loop with aging (prevents starvation)."""
        print("Resource allocator started with FAIR scheduling")

        while not self.exit_flag.is_set():
            try:

                self._update_internal_queue()


                can_allocate = len(self.active_processes) < self.resource_units
                if can_allocate and self.priority_queue:

                    chosen_req = self._choose_next_request_fair()
                    if chosen_req is not None:
                        process_id = chosen_req["process_id"]
                        base_priority = chosen_req["base_priority"]
                        work_time = chosen_req["work_time"]
                        arrival_time = chosen_req["arrival_time"]


                        self.priority_queue.remove(chosen_req)


                        self.active_processes.add(process_id)
                        self.last_served_process = process_id


                        wait_time = time.time() - arrival_time
                        if process_id in self.waiting_times:
                            self.waiting_times[process_id].append(wait_time)
                        else:
                            self.waiting_times[process_id] = [wait_time]


                        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        log_entry = (
                            f"{timestamp} - Process {process_id} "
                            f"(BasePriority: {base_priority}) granted resource after waiting {wait_time:.2f}s "
                            f"[FAIR]"
                        )
                        self.resource_access_logs.append(log_entry)
                        print(log_entry)


                        time.sleep(work_time)


                        self.active_processes.remove(process_id)


                        completion_time = time.time()
                        if process_id in self.completion_times:
                            self.completion_times[process_id].append(completion_time)
                        else:
                            self.completion_times[process_id] = [completion_time]

                        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        log_entry = (
                            f"{timestamp} - Process {process_id} finished task, "
                            f"held resource for {work_time:.2f}s"
                        )
                        self.resource_access_logs.append(log_entry)
                        print(log_entry)


                time.sleep(0.05)

            except Exception as e:
                print(f"Error in resource allocator: {e}")

        print("Resource allocator stopped")

    def request_resource(self, process_id, priority, work_time):
        """
        Request access to the resource.

        Args:
            process_id: Unique identifier for the process
            priority: Priority level (higher number = higher priority)
            work_time: How long the process needs the resource
        """
        req_time = time.time()
        self.process_start_times[process_id] = req_time


        self.process_queue.put((process_id, priority, work_time, req_time))

        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = (
            f"{timestamp} - Process {process_id} (Priority: {priority}) "
            f"requested resource for {work_time:.2f}s"
        )
        self.resource_access_logs.append(log_entry)
        print(log_entry)


def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    """
    Worker process that requests resources multiple times.
    """
    try:
        for _ in range(work_iterations):
            work_time = random.uniform(0.5, 2.0)


            resource_manager.request_resource(process_id, priority, work_time)


            delay = random.uniform(delay_range[0], delay_range[1])
            time.sleep(delay)

    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")


def analyze_results(resource_manager):
    """Analyze and display the results of the simulation."""
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)


    avg_waiting_times = {}
    for pid, waits in waiting_times.items():
        avg_waiting_times[pid] = sum(waits) / len(waits)


    tasks_completed = {}
    for pid, comps in completion_times.items():
        tasks_completed[pid] = len(comps)

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
        print("------------------")
        for pid in starved_processes:
            print(
                f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s "
                "(exceeded threshold)"
            )


    process_ids = sorted(avg_waiting_times.keys())
    avg_waits = [avg_waiting_times[pid] for pid in process_ids]

    plt.figure(figsize=(10, 6))
    plt.bar(
        process_ids,
        avg_waits,
        color=['red' if pid in starved_processes else 'blue' for pid in process_ids],
    )
    plt.title('Average Waiting Time by Process (Fair Scheduler)')
    plt.xlabel('Process ID')
    plt.ylabel('Average Waiting Time (seconds)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('waiting_times.png')

    tasks = [tasks_completed.get(pid, 0) for pid in process_ids]

    plt.figure(figsize=(10, 6))
    plt.bar(
        process_ids,
        tasks,
        color=['green' if tasks_completed.get(pid, 0) > 1 else 'orange' for pid in process_ids],
    )
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
    """Main function to run the simulation."""
    num_processes = 5
    resource_units = 1
    simulation_duration = 30

    resource_manager = ResourceManager(resource_units=resource_units, aging_rate=0.5)


    allocator_thread = threading.Thread(target=resource_manager.allocate_resources)
    allocator_thread.daemon = True
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

    print(f"Starting simulation with {num_processes} processes using FAIR scheduling...")

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

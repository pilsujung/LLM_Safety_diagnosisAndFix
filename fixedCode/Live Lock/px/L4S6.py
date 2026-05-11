import threading
import time
import random
from enum import Enum
import logging
from collections import defaultdict, deque

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('RobotSimulation')


resource_state_lock = threading.RLock()

class ResourceType(Enum):
    CHARGER = "Charging Station"
    TOOL = "Tool"
    WORKSTATION = "Workstation"

class Resource:
    def __init__(self, resource_id, resource_type):
        self.id = resource_id
        self.type = resource_type
        self.lock = threading.Lock()
        self.owner = None
        self.requested_by = []

    def try_acquire(self, robot):
        with resource_state_lock:
            if self.lock.acquire(blocking=False):
                self.owner = robot
                logger.info(f"Robot {robot.id} acquired {self.type.value} {self.id}")
                robot._mark_progress()
                return True
            else:
                if robot not in self.requested_by:
                    self.requested_by.append(robot)
                logger.info(f"Robot {robot.id} waiting for {self.type.value} {self.id} (owned by Robot {self.owner.id if self.owner else 'None'})")
                return False

    def release(self, robot):
        with resource_state_lock:
            if self.owner == robot:
                self.owner = None
                if robot in self.requested_by:
                    self.requested_by.remove(robot)
                self.lock.release()
                logger.info(f"Robot {robot.id} released {self.type.value} {self.id}")
                robot._mark_progress()
                return True
            return False

    def __str__(self):
        return f"{self.type.value} {self.id}"

class Robot:
    def __init__(self, robot_id, resources_needed):
        self.id = robot_id
        self.resources_needed = sorted(resources_needed, key=lambda r: (r.type.value, r.id))
        self.resources_held = []
        self.status = "idle"
        self.last_progress_time = time.monotonic()
        self.give_up_resource_probability = 0.0
        self.attempt_count = 0

    def _mark_progress(self):
        self.last_progress_time = time.monotonic()
        self.attempt_count = 0

    def run(self):
        while True:
            all_acquired = True
            

            if random.random() < self.give_up_resource_probability and self.resources_held:
                resource = min(self.resources_held, key=lambda r: (r.type.value, r.id))
                logger.warning(f"Robot {self.id} strategically releasing {resource}")
                resource.release(self)
                self.resources_held.remove(resource)
                time.sleep(random.uniform(0.3, 0.8))
                continue


            with resource_state_lock:
                for resource in self.resources_needed:
                    if resource not in self.resources_held:
                        if resource.try_acquire(self):
                            self.resources_held.append(resource)
                        else:
                            all_acquired = False

            if all_acquired and len(self.resources_held) == len(self.resources_needed):
                old_status = self.status
                self.status = "working"
                if old_status != "working":
                    logger.info(f"Robot {self.id} is now working with all required resources")
                self._mark_progress()
                time.sleep(random.uniform(0.5, 2.0))


                for resource in self.resources_held.copy():
                    resource.release(self)
                    self.resources_held.remove(resource)

                self.status = "idle"
                logger.info(f"Robot {self.id} completed work cycle")
                time.sleep(random.uniform(0.2, 1.0))
            else:
                self.status = "waiting"
                self.attempt_count += 1
                backoff = min(0.1 * (2 ** min(self.attempt_count // 10, 5)), 1.0)
                time.sleep(backoff + random.uniform(0, 0.05))

class LivelockMonitor:
    def __init__(self, robots, livelock_threshold=8.0, on_detect=None):
        self.robots = robots
        self.livelock_threshold = livelock_threshold
        self.livelock_detected = False
        self.on_detect = on_detect

    def _all_waiting_and_stalled(self):
        now = time.monotonic()
        return all(
            r.status == "waiting" and (now - r.last_progress_time) > self.livelock_threshold
            for r in self.robots
        )

    def _build_wait_for_graph(self):
        with resource_state_lock:
            graph = defaultdict(set)
            waits = defaultdict(list)
            holds = defaultdict(list)

            for r in self.robots:
                holds[r.id] = [str(res) for res in r.resources_held]
                for res in r.resources_needed:
                    if res not in r.resources_held and res.owner:
                        graph[r.id].add(res.owner.id)
                        waits[r.id].append(f"{res} (by R{res.owner.id})")
            return graph, waits, holds

    def _find_cycle(self, graph):
        visited = {}
        parent = {}

        def dfs(u):
            visited[u] = 1
            for v in graph[u]:
                if v not in visited:
                    parent[v] = u
                    if dfs(v): return True
                elif visited[v] == 1:
                    return True
            visited[u] = 2
            return False

        nodes = set(graph) | set(v for s in graph.values() for v in s)
        for node in nodes:
            if node not in visited:
                if dfs(node):
                    return True
        return False

    def check_for_livelock(self):
        if not self._all_waiting_and_stalled():
            self.livelock_detected = False
            return False

        graph, waits, holds = self._build_wait_for_graph()
        if not self._find_cycle(graph):
            return False

        if not self.livelock_detected:
            self.livelock_detected = True
            logger.critical("🔴 LIVELOCK DETECTED 🔴")
            for r in self.robots:
                logger.critical(f"R{r.id}: holds [{', '.join(holds[r.id]) or '-'}], waits [{', '.join(waits[r.id]) or '-'}]")

        self.on_detect(self.robots)
        return True

    def run(self):
        while True:
            self.check_for_livelock()
            time.sleep(1.0)

def livelock_resolution_strategy(robots, cycle_nodes=None):
    for robot in robots:
        robot.give_up_resource_probability = 0.7
    logger.warning("Livelock resolution: Increased release probability")

def simulate_production_line(duration=30):
    charger1 = Resource(1, ResourceType.CHARGER)
    charger2 = Resource(2, ResourceType.CHARGER)
    tool1 = Resource(1, ResourceType.TOOL)
    tool2 = Resource(2, ResourceType.TOOL)
    workstation = Resource(1, ResourceType.WORKSTATION)

    robot1 = Robot(1, [charger1, tool1, workstation])
    robot2 = Robot(2, [charger1, tool2, workstation])
    robot3 = Robot(3, [charger2, tool1, workstation])

    robots = [robot1, robot2, robot3]
    monitor = LivelockMonitor(robots, on_detect=livelock_resolution_strategy)

    threads = [threading.Thread(target=r.run, daemon=True) for r in robots]
    monitor_thread = threading.Thread(target=monitor.run, daemon=True)

    for t in threads: t.start()
    monitor_thread.start()

    time.sleep(duration)
    logger.info("Simulation completed - CHECK FOR WORK CYCLES ABOVE")

if __name__ == "__main__":
    logger.info("Starting FIXED robot simulation")
    simulate_production_line(duration=30)

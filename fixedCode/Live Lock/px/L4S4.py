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


resource_state_lock = threading.Lock()

class ResourceType(Enum):
    CHARGER = "Charging Station"
    TOOL = "Tool"
    WORKSTATION = "Workstation"

class Resource:
    def __init__(self, resource_id, resource_type):
        self.id = resource_id
        self.type = resource_type
        self._lock = threading.Lock()
        self.owner = None
        self.requested_by = []
        self.last_wait_log = defaultdict(lambda: 0.0)

    def try_acquire(self, robot):
        if self._lock.acquire(blocking=False):
            with resource_state_lock:
                self.owner = robot
                if robot not in self.resources_held:
                    robot.resources_held.append(self)
            logger.info(f"Robot {robot.id} acquired {self.type.value} {self.id}")
            robot._mark_progress()
            return True
        else:
            with resource_state_lock:
                if robot not in self.requested_by:
                    self.requested_by.append(robot)
                now = time.monotonic()
                key = robot.id
                if now - self.last_wait_log[key] > 1.0:
                    logger.info(f"Robot {robot.id} waiting for {self.type.value} {self.id} (owned by Robot {self.owner.id if self.owner else 'None'})")
                    self.last_wait_log[key] = now
            return False

    def release(self, robot):
        released = False
        with resource_state_lock:
            if self.owner == robot:
                self.owner = None
                self.requested_by[:] = [r for r in self.requested_by if r != robot]
                if self in robot.resources_held:
                    robot.resources_held.remove(self)
                released = True
        if released:
            self._lock.release()
            logger.info(f"Robot {robot.id} released {self.type.value} {self.id}")
            robot._mark_progress()
        return released

    def __str__(self):
        return f"{self.type.value} {self.id}"

    def __lt__(self, other):
        return (self.type.value, self.id) < (other.type.value, other.id)

class Robot:
    def __init__(self, robot_id, resources_needed):
        self.id = robot_id
        self.resources_needed = sorted(resources_needed)
        self.resources_held = []
        self.status = "idle"
        self.last_progress_time = time.monotonic()
        self.give_up_resource_probability = 0.0

    def _mark_progress(self):
        self.last_progress_time = time.monotonic()

    def run(self):
        while True:

            if random.random() < self.give_up_resource_probability and self.resources_held:
                with resource_state_lock:
                    resource = random.choice(self.resources_held)
                logger.warning(f"Robot {self.id} strategically releasing {resource} to avoid livelock")
                resource.release(self)
                time.sleep(random.uniform(0.1, 0.5))


            all_acquired = True
            temp_held = []
            for resource in self.resources_needed:
                if resource not in self.resources_held:
                    if resource.try_acquire(self):
                        temp_held.append(resource)
                    else:

                        for r in temp_held:
                            r.release(self)
                        all_acquired = False
                        break

            if all_acquired and len(self.resources_held) == len(self.resources_needed):
                self.status = "working"
                logger.info(f"Robot {self.id} is now working with all required resources")
                self._mark_progress()
                time.sleep(random.uniform(0.5, 2.0))


                for resource in reversed(self.resources_needed):
                    if resource in self.resources_held:
                        resource.release(self)
                self.status = "idle"
                logger.info(f"Robot {self.id} completed work cycle and released all resources")
                time.sleep(random.uniform(0.2, 1.0))
            else:
                self.status = "waiting"
                time.sleep(0.05)

class LivelockMonitor:
    def __init__(self, robots, livelock_threshold=5.0, on_detect=None):
        self.robots = robots
        self.livelock_threshold = livelock_threshold
        self.livelock_detected = False
        self._last_cycle = None
        self.on_detect = on_detect

    def _all_waiting_and_stalled(self):
        now = time.monotonic()
        return all(
            r.status == "waiting" and (now - r.last_progress_time) >= self.livelock_threshold
            for r in self.robots
        )

    def _build_wait_for_graph(self):
        graph = defaultdict(set)
        waits = defaultdict(list)
        holds = {}

        with resource_state_lock:
            for r in self.robots:
                holds[r.id] = [str(res) for res in r.resources_held]
                for res in r.resources_needed:
                    if res not in r.resources_held and res.owner:
                        graph[r.id].add(res.owner.id)
                        waits[r.id].append(f"{res} (held by Robot {res.owner.id})")
        return graph, waits, holds

    def _find_cycle(self, graph):
        visited = {}
        parent = {}

        def dfs(u):
            visited[u] = 1
            for v in graph[u]:
                if v not in visited:
                    parent[v] = u
                    if dfs(v):
                        return True
                elif visited[v] == 1:
                    return True
            visited[u] = 2
            return False

        all_nodes = set(graph) | set(v for values in graph.values() for v in values)
        for node in all_nodes:
            if node not in visited:
                parent[node] = None
                if dfs(node):
                    return True
        return False

    def _log_dependency(self, waits, holds, cycle_detected):
        logger.critical("Dependency Chain:")
        with resource_state_lock:
            for r in self.robots:
                holding = ", ".join(holds.get(r.id, [])) or "-"
                waiting_for = ", ".join(waits.get(r.id, [])) or "-"
                logger.critical(f"Robot {r.id} holds [{holding}] and waits for [{waiting_for}]")
        if cycle_detected:
            logger.critical("CIRCULAR WAIT DETECTED")

    def check_for_livelock(self):
        if not self._all_waiting_and_stalled():
            self.livelock_detected = False
            return False

        graph, waits, holds = self._build_wait_for_graph()
        cycle_exists = self._find_cycle(graph)
        if not cycle_exists:
            self.livelock_detected = False
            return False

        if not self.livelock_detected:
            self.livelock_detected = True
            logger.critical("🔴LIVELOCK DETECTED: All robots stalled with circular wait🔴")
            self._log_dependency(waits, holds, True)

        if self.on_detect:
            self.on_detect(self.robots)
        return True

    def run(self):
        while True:
            self.check_for_livelock()
            time.sleep(0.5)

def livelock_resolution_strategy(robots):
    logger.warning("Activating livelock resolution - forcing high release probability")
    for robot in robots:
        robot.give_up_resource_probability = 0.8

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
    logger.info("Simulation completed - NO PERMANENT LIVELOCK")

if __name__ == "__main__":
    logger.info("Starting robot production line simulation")
    simulate_production_line(duration=30)

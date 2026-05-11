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

class ResourceType(Enum):
    CHARGER = "Charging Station"
    TOOL = "Tool"
    WORKSTATION = "Workstation"

class Resource:
    def __init__(self, resource_id, resource_type):
        self.id = resource_id
        self.type = resource_type
        self.priority = (resource_type.value, resource_id)
        self.lock = threading.Lock()
        self.owner = None

    def try_acquire(self, robot):
        if self.lock.acquire(blocking=False):
            self.owner = robot
            logger.info(f"Robot {robot.id} acquired {self.type.value} {self.id}")
            robot._mark_progress()
            return True
        return False

    def release(self, robot):
        if self.owner == robot:
            self.owner = None
            self.lock.release()
            logger.info(f"Robot {robot.id} released {self.type.value} {self.id}")
            robot._mark_progress()
            return True
        return False

    def __lt__(self, other):
        return self.priority < other.priority

    def __str__(self):
        return f"{self.type.value} {self.id}"

class Robot:
    def __init__(self, robot_id, resources_needed):
        self.id = robot_id
        self.resources_needed = sorted(resources_needed, key=lambda r: r.priority)
        self.resources_held = []
        self.status = "idle"
        self.last_progress_time = time.monotonic()
        self.retry_count = 0
        self.max_retries = 50

    def _mark_progress(self):
        self.last_progress_time = time.monotonic()
        self.retry_count = 0

    def run(self):
        while True:

            if self.retry_count > self.max_retries and self.resources_held:
                resource = min(self.resources_held, key=lambda r: r.priority)
                logger.warning(f"Robot {self.id} force-releasing {resource} after {self.retry_count} retries")
                resource.release(self)
                self.resources_held.remove(resource)
                self.retry_count = 0
                time.sleep(random.uniform(0.5, 1.0))
                continue

            all_acquired = True
            for resource in self.resources_needed:
                if resource not in self.resources_held:
                    if resource.try_acquire(self):
                        self.resources_held.append(resource)
                    else:
                        all_acquired = False
                        self.retry_count += 1
                        break

            if all_acquired:
                self.status = "working"
                logger.info(f"Robot {self.id} is now working with all required resources")
                time.sleep(random.uniform(0.5, 2.0))
                

                for resource in reversed(self.resources_held):
                    resource.release(self)
                self.resources_held.clear()
                self.status = "idle"
                logger.info(f"Robot {self.id} completed work cycle")
                time.sleep(random.uniform(0.2, 0.5))
            else:
                self.status = "waiting"

                backoff = min(0.1 * (2 ** (self.retry_count // 10)), 2.0) + random.uniform(0, 0.1)
                time.sleep(backoff)

class LivelockMonitor:
    def __init__(self, robots, livelock_threshold=10.0):
        self.robots = robots
        self.livelock_threshold = livelock_threshold
        self.livelock_detected = False

    def _all_waiting_and_stalled(self):
        now = time.monotonic()
        return all(
            r.status == "waiting" and (now - r.last_progress_time) > self.livelock_threshold
            for r in self.robots
        )

    def _build_wait_for_graph(self):
        graph = defaultdict(set)
        for r in self.robots:
            for res in r.resources_needed:
                if res not in r.resources_held and res.owner and res.owner != r:
                    graph[r.id].add(res.owner.id)
        return graph

    def _find_cycle(self, graph):

        visited = set()
        rec_stack = set()
        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            for neighbor in graph[node]:
                if neighbor not in visited:
                    if dfs(neighbor): return True
                elif neighbor in rec_stack: return True
            rec_stack.remove(node)
            return False
        for node in graph:
            if node not in visited and dfs(node):
                return True
        return False

    def check_for_livelock(self):
        if not self._all_waiting_and_stalled():
            return False
        graph = self._build_wait_for_graph()
        if self._find_cycle(graph):
            logger.critical("🔴 LIVELOCK DETECTED - Force reset all resources 🔴")

            for robot in self.robots:
                for resource in robot.resources_held[:]:
                    resource.release(robot)
                    robot.resources_held.remove(resource)
            return True
        return False

    def run(self):
        while True:
            self.check_for_livelock()
            time.sleep(1.0)

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
    monitor = LivelockMonitor(robots)

    threads = [threading.Thread(target=r.run, daemon=True) for r in robots]
    monitor_thread = threading.Thread(target=monitor.run, daemon=True)

    for t in threads: t.start()
    monitor_thread.start()

    time.sleep(duration)
    logger.info("Simulation completed - CHECK FOR WORK CYCLES ABOVE")

if __name__ == "__main__":
    logger.info("Starting FIXED robot production line simulation")
    simulate_production_line()

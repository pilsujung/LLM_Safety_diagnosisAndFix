import threading
import time
import random
from enum import Enum
import logging
from collections import defaultdict

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
        self.lock = threading.Lock()
        self.owner = None
        self.requested_by = []
        self.requested_by_lock = threading.Lock()
    
    def try_acquire(self, robot):
        """Attempt to acquire the resource without blocking"""
        if self.lock.acquire(blocking=False):
            self.owner = robot
            logger.info(f"Robot {robot.id} acquired {self.type.value} {self.id}")
            robot._mark_progress()
            return True
        else:
            with self.requested_by_lock:
                if robot not in self.requested_by:
                    self.requested_by.append(robot)
            logger.info(f"Robot {robot.id} waiting for {self.type.value} {self.id} (owned by Robot {self.owner.id if self.owner else 'None'})")
            return False
    
    def release(self, robot):
        """Release the resource if owned by the robot"""
        if self.owner == robot:
            self.owner = None
            with self.requested_by_lock:
                if robot in self.requested_by:
                    self.requested_by.remove(robot)
            self.lock.release()
            logger.info(f"Robot {robot.id} released {self.type.value} {self.id}")
            robot._mark_progress()
            return True
        return False
    
    def __str__(self):
        return f"{self.type.value} {self.id}"
    
    def __lt__(self, other):
        """Define ordering for resources to ensure consistent acquisition order"""
        if self.type.value != other.type.value:
            return self.type.value < other.type.value
        return self.id < other.id

class Robot:
    def __init__(self, robot_id, resources_needed):
        self.id = robot_id

        self.resources_needed = sorted(resources_needed)
        self.resources_held = []
        self.status = "idle"
        self.last_progress_time = time.monotonic()
        self.failed_attempts = 0
        self.backoff_time = 0.1
    
    def _mark_progress(self):
        self.last_progress_time = time.monotonic()
        self.failed_attempts = 0
        self.backoff_time = 0.1

    def _try_acquire_all_resources(self):
        """Try to acquire all resources atomically (all or nothing)"""
        acquired = []
        

        for resource in self.resources_needed:
            if resource.try_acquire(self):
                acquired.append(resource)
            else:

                for res in acquired:
                    res.release(self)
                return False
        

        self.resources_held = acquired
        return True
    
    def run(self):
        """Main robot operation loop"""
        while True:

            if self._try_acquire_all_resources():
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
                logger.info(f"Robot {self.id} completed work cycle and released all resources")
                time.sleep(random.uniform(0.2, 1.0))
            else:

                self.status = "waiting"
                self.failed_attempts += 1
                

                if self.failed_attempts > 3:
                    self.backoff_time = min(self.backoff_time * 1.5, 2.0)
                
                backoff = self.backoff_time + random.uniform(0, self.backoff_time * 0.5)
                time.sleep(backoff)

class LivelockMonitor:
    def __init__(self, robots, livelock_threshold=5.0, on_detect=None):
        self.robots = robots
        self.livelock_threshold = livelock_threshold
        self.livelock_detected = False
        self._last_cycle = None
        self.on_detect = on_detect 

    def _all_waiting_and_stalled(self):
        now = time.monotonic()
        all_waiting = True
        all_exceeded = True
        for r in self.robots:
            if r.status != "waiting":
                all_waiting = False
                break
            if (now - r.last_progress_time) < self.livelock_threshold:
                all_exceeded = False
        return all_waiting and all_exceeded

    def _build_wait_for_graph(self):
        """
        Returns:
            graph: dict[int, set[int]] - edges i -> j if robot i waits for a resource owned by robot j
            waits: dict[int, list[str]] - human-readable waits for logging
            holds: dict[int, list[str]] - resources each robot holds
        """
        graph = defaultdict(set)
        waits = defaultdict(list)
        holds = defaultdict(list)

        for r in self.robots:
            holds[r.id] = [str(res) for res in r.resources_held]
            for res in r.resources_needed:
                if res not in r.resources_held and res.owner is not None:
                    owner_id = res.owner.id
                    graph[r.id].add(owner_id)
                    waits[r.id].append(f"{res} (held by Robot {owner_id})")

        return graph, waits, holds

    def _find_cycle(self, graph):
        visited = {}
        parent = {}

        def dfs(u):
            visited[u] = 1
            for v in graph[u]:
                if v not in visited:
                    parent[v] = u
                    found = dfs(v)
                    if found:
                        return found
                elif visited[v] == 1:
                    cycle = [v]
                    cur = u
                    while cur != v:
                        cycle.append(cur)
                        cur = parent[cur]
                    cycle.reverse()
                    return cycle
            visited[u] = 2
            return None

        for node in list({rid for rid in graph} | {v for s in graph.values() for v in s}):
            if node not in visited:
                parent[node] = None
                found = dfs(node)
                if found:
                    return found
        return None

    def _log_dependency(self, waits, holds, cycle_nodes=None):
        logger.critical("Dependency Chain:")
        for r in self.robots:
            holding = ", ".join(holds.get(r.id, [])) or "-"
            waiting_for = ", ".join(waits.get(r.id, [])) or "-"
            logger.critical(f"Robot {r.id} holds [{holding}] and waits for [{waiting_for}]")
        if cycle_nodes:
            cycle_str = " -> ".join(f"Robot {rid}" for rid in cycle_nodes) + " -> (back to start)"
            logger.critical(f"Wait-for cycle detected: {cycle_str}")

    def check_for_livelock(self):
        if not self._all_waiting_and_stalled():
            self.livelock_detected = False
            self._last_cycle = None
            return False

        graph, waits, holds = self._build_wait_for_graph()
        cycle_nodes = self._find_cycle(graph)
        if cycle_nodes is None:
            self.livelock_detected = False
            self._last_cycle = None
            return False

        if not self.livelock_detected:
            self.livelock_detected = True
            self._last_cycle = cycle_nodes
            logger.critical("🔴LIVELOCK DETECTED: All robots are stalled and a wait-for cycle exists🔴")
            self._log_dependency(waits, holds, cycle_nodes)

            if callable(self.on_detect):
                try:
                    self.on_detect(self.robots, cycle_nodes)
                except Exception as e:
                    logger.exception(f"Error running livelock resolution hook: {e}")
            return True

        return False

    def run(self):
        """Run the livelock detection loop"""
        while True:
            self.check_for_livelock()
            time.sleep(0.5)

def introduce_livelock_condition(robots):
    """This function is now a no-op since we've fixed the livelock"""
    pass

def livelock_resolution_strategy(robots, cycle_nodes=None):
    """Fallback strategy - shouldn't be needed with the fixes"""
    logger.warning("Livelock resolution strategy called (this shouldn't happen with fixed code)")
    return True

def simulate_production_line(duration=60):
    """Set up and run the simulation"""
    charger1 = Resource(1, ResourceType.CHARGER)
    charger2 = Resource(2, ResourceType.CHARGER)
    tool1 = Resource(1, ResourceType.TOOL)
    tool2 = Resource(2, ResourceType.TOOL)
    workstation = Resource(1, ResourceType.WORKSTATION)
    
    robot1 = Robot(1, [charger1, tool1, workstation])
    robot2 = Robot(2, [charger1, tool2, workstation])
    robot3 = Robot(3, [charger2, tool1, workstation])
    
    robots = [robot1, robot2, robot3]
    
    monitor = LivelockMonitor(robots, livelock_threshold=5.0, on_detect=livelock_resolution_strategy)
    
    threads = []
    for robot in robots:
        thread = threading.Thread(target=robot.run, daemon=True)
        threads.append(thread)
        thread.start()
    
    monitor_thread = threading.Thread(target=monitor.run, daemon=True)
    monitor_thread.start()
    
    introduce_livelock_condition(robots)
    
    start = time.monotonic()
    while time.monotonic() - start < duration:
        time.sleep(0.5)
    
    logger.info("Simulation completed")

if __name__ == "__main__":
    logger.info("Starting robot production line simulation")
    simulate_production_line(duration=20)
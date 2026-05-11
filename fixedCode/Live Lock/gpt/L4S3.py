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
        self.lock = threading.Lock()
        self.owner = None
        self.requested_by = []

    def try_acquire(self, robot):
        """Attempt to acquire the resource without blocking"""
        if self.lock.acquire(blocking=False):
            self.owner = robot
            logger.info(f"Robot {robot.id} acquired {self.type.value} {self.id}")
            robot._mark_progress()
            return True
        else:
            if robot not in self.requested_by:
                self.requested_by.append(robot)
            logger.info(
                f"Robot {robot.id} waiting for {self.type.value} {self.id} "
                f"(owned by Robot {self.owner.id if self.owner else 'None'})"
            )
            return False

    def release(self, robot):
        """Release the resource if owned by the robot"""
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
        self.resources_needed = resources_needed
        self.resources_held = []
        self.status = "idle"
        self.last_progress_time = time.monotonic()

    def _mark_progress(self):
        self.last_progress_time = time.monotonic()

    def run(self):
        """
        Main robot operation loop with atomic-style resource acquisition.

        Key idea:
        - Each attempt either acquires ALL required resources or NONE.
        - Partial acquisitions are rolled back immediately.
        - Resources are acquired in a global deterministic order to avoid cycles.
        """
        while True:

            self.status = "waiting"

            acquired_this_round = []


            ordered_resources = sorted(
                self.resources_needed,
                key=lambda r: (r.type.name, r.id)
            )


            for resource in ordered_resources:
                if resource.try_acquire(self):
                    acquired_this_round.append(resource)
                else:

                    for res in acquired_this_round:
                        res.release(self)
                    acquired_this_round.clear()


                    time.sleep(random.uniform(0.1, 0.3))
                    break


            if len(acquired_this_round) != len(ordered_resources):
                time.sleep(0.05)
                continue


            self.resources_held = acquired_this_round

            old_status = self.status
            self.status = "working"
            if old_status != "working":
                logger.info(f"Robot {self.id} is now working with all required resources")

            self._mark_progress()
            time.sleep(random.uniform(0.5, 2.0))


            for resource in self.resources_held:
                resource.release(self)
            self.resources_held = []

            self.status = "idle"
            logger.info(f"Robot {self.id} completed work cycle and released all resources")


            time.sleep(random.uniform(0.2, 1.0))


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

        nodes = list({rid for rid in graph} | {v for s in graph.values() for v in s})
        for node in nodes:
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
    """
    Previously increased livelock probability by tweaking behavior.
    With atomic acquisition, we no longer deliberately introduce livelock.
    """
    logger.info("introduce_livelock_condition: no-op (algorithm now prevents livelock structurally).")


def livelock_resolution_strategy(robots, cycle_nodes=None):
    """
    Strategy hook invoked on livelock detection.

    With the atomic, ordered acquisition, livelock should not occur in practice.
    We keep this hook for logging / experimentation.
    """
    logger.warning("Livelock resolution strategy invoked (this should be rare with the new algorithm).")
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

    monitor = LivelockMonitor(
        robots,
        livelock_threshold=5.0,
        on_detect=livelock_resolution_strategy
    )

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

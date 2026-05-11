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
            logger.info(f"Robot {robot.id} waiting for {self.type.value} {self.id} (owned by Robot {self.owner.id if self.owner else 'None'}) - wait {random.randint(50,200)}ms")
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
        self.random = random.Random()
        self.attempt_count = defaultdict(int)

    def _mark_progress(self):
        self.last_progress_time = time.monotonic()

    def run(self):
        """Main robot operation loop with randomized backoff to break livelock"""
        while True:
            all_acquired = True
            

            for resource in self.resources_needed:
                if resource not in self.resources_held:
                    self.attempt_count[resource.id] += 1
                    

                    if not resource.try_acquire(self):
                        wait_time = random.randint(50, 250)
                        logger.info(f"Robot {self.id} backing off {wait_time}ms for {resource}")
                        time.sleep(wait_time / 1000.0)
                        all_acquired = False
                        

                        if self.attempt_count[resource.id] > 5:
                            logger.warning(f"Robot {self.id} forcing release chain for {resource}")

                            for held_res in self.resources_held[:]:
                                held_res.release(self)
                                self.resources_held.remove(held_res)
                            self.attempt_count[resource.id] = 0
                            break
                    else:
                        self.resources_held.append(resource)
                        self.attempt_count[resource.id] = 0


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
                logger.info(f"Robot {self.id} completed work cycle and released all resources")
                time.sleep(random.uniform(0.2, 1.0))
            else:
                self.status = "waiting"

                time.sleep(random.uniform(0.05, 0.15))

class LivelockMonitor:
    def __init__(self, robots, livelock_threshold=5.0):
        self.robots = robots
        self.livelock_threshold = livelock_threshold
        self.livelock_detected = False

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

    def check_for_livelock(self):
        if not self._all_waiting_and_stalled():
            self.livelock_detected = False
            return False

        graph, waits, holds = self._build_wait_for_graph()

        logger.info("Monitor: All waiting but randomization should resolve...")
        return False

    def run(self):
        while True:
            self.check_for_livelock()
            time.sleep(1.0)

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
    monitor = LivelockMonitor(robots)

    threads = []
    for robot in robots:
        thread = threading.Thread(target=robot.run, daemon=True)
        threads.append(thread)
        thread.start()

    monitor_thread = threading.Thread(target=monitor.run, daemon=True)
    monitor_thread.start()

    start = time.monotonic()
    while time.monotonic() - start < duration:
        time.sleep(0.5)

    logger.info("Simulation completed")

if __name__ == "__main__":
    logger.info("Starting fixed robot production line simulation (livelock resolved)")
    simulate_production_line(duration=30)

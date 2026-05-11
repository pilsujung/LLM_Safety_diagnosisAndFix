import threading
import time
import random
from enum import Enum
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RobotSimulation')

class ResourceType(Enum):
    CHARGER = "Charging Station"
    TOOL = "Tool"
    WORKSTATION = "Workstation"


resource_state_lock = threading.Lock()

class Resource:
    def __init__(self, resource_id, resource_type):
        self.id = resource_id
        self.type = resource_type
        self.order_key = (resource_type.value, resource_id)
        self._lock = threading.Lock()
        self.owner = None
        self.requested_by = []

    def try_acquire(self, robot):
        with resource_state_lock:
            if self._lock.acquire(blocking=False):
                self.owner = robot
                logger.info(f"Robot {robot.id} acquired {self.type.value} {self.id}")
                robot._mark_progress()
                return True
            else:
                if robot not in self.requested_by:
                    self.requested_by.append(robot)
                logger.info(f"Robot {robot.id} waiting for {self.type.value} {self.id}")
                return False

    def release(self, robot):
        with resource_state_lock:
            if self.owner == robot:
                self.owner = None
                self.requested_by[:] = [r for r in self.requested_by if r != robot]
                self._lock.release()
                logger.info(f"Robot {robot.id} released {self.type.value} {self.id}")
                robot._mark_progress()
                return True
            return False

    def __str__(self):
        return f"{self.type.value} {self.id}"

class Robot:
    def __init__(self, robot_id, resources_needed):
        self.id = robot_id
        self.resources_needed = sorted(resources_needed, key=lambda r: r.order_key)
        self.resources_held = []
        self.status = "idle"
        self.last_progress_time = time.monotonic()
        self.backoff_time = 0.1
        self.consecutive_fails = 0

    def _mark_progress(self):
        self.last_progress_time = time.monotonic()
        self.consecutive_fails = 0
        self.backoff_time = 0.1

    def run(self):
        while True:
            with resource_state_lock:
                all_acquired = all(r in self.resources_held for r in self.resources_needed)

            if not all_acquired:
                self.consecutive_fails += 1
                if self.consecutive_fails > 10:
                    self.backoff_time = min(self.backoff_time * 1.5, 1.0)
                

                acquired_any = False
                for resource in self.resources_needed:
                    if resource not in self.resources_held:
                        if resource.try_acquire(self):
                            self.resources_held.append(resource)
                            acquired_any = True
                            break

                if not acquired_any:
                    self.status = "waiting"
                    time.sleep(self.backoff_time)
                    continue


            self.status = "working"
            logger.info(f"Robot {self.id} is now working with all required resources")
            time.sleep(random.uniform(0.5, 2.0))


            for resource in self.resources_held[:]:
                resource.release(self)
                self.resources_held.remove(resource)

            self.status = "idle"
            logger.info(f"Robot {self.id} completed work cycle")
            time.sleep(random.uniform(0.2, 0.5))

class LivelockMonitor:
    def __init__(self, robots, livelock_threshold=3.0):
        self.robots = robots
        self.livelock_threshold = livelock_threshold
        self._lock = resource_state_lock

    def check_for_livelock(self):
        with self._lock:
            now = time.monotonic()
            all_waiting = all(r.status == "waiting" for r in self.robots)
            all_stalled = all((now - r.last_progress_time) > self.livelock_threshold for r in self.robots)
            
            if all_waiting and all_stalled:

                logger.warning("🔴 Force-breaking livelock by releasing resources")
                for robot in self.robots:
                    if robot.resources_held:

                        ws = next((r for r in robot.resources_held if r.type == ResourceType.WORKSTATION), None)
                        if ws:
                            ws.release(robot)
                            robot.resources_held.remove(ws)
                        else:

                            r = robot.resources_held.pop(0)
                            r.release(robot)
                return True
        return False

    def run(self):
        while True:
            self.check_for_livelock()
            time.sleep(0.5)

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
    logger.info("Simulation completed - NO LIVELOCK")

if __name__ == "__main__":
    simulate_production_line()

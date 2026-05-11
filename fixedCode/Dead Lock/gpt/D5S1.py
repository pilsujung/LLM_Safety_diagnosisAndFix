import threading
import time
import random
from typing import Dict, List
import logging

class Resource:
    def __init__(self, name: str, value: int = 100):
        self.name = name
        self.value = value
        self.lock = threading.Lock()
        self.is_available = True

class Worker(threading.Thread):
    def __init__(self, name: str, resources: List[Resource], manager):
        super().__init__(name=name, daemon=True)
        self.resources = resources
        self.manager = manager
        self.resources_held: List[Resource] = []

    def run(self):
        while self.manager.is_running:
            try:
                if not self.acquire_resources():
                    
                    continue
                self.perform_work()
            except Exception as e:
                logging.error(f"Worker {self.name} encountered error: {e}")
            finally:
                self.release_resources()
                time.sleep(random.uniform(0.05, 0.2))

    def acquire_resources(self) -> bool:
        """
        Acquire all required resources in a consistent (sorted) order.
        If any acquisition times out, roll back and retry.
        """
        
        ordered = sorted(self.resources, key=lambda r: r.name)

        
        for _ in range(1000):
            if not self.manager.is_running:
                return False

            acquired_all = True
            self.resources_held.clear()

            for resource in ordered:
                logging.info(f"Worker {self.name} attempting to acquire {resource.name}")
                
                got = resource.lock.acquire(timeout=0.3 + random.uniform(0.0, 0.2))
                if got:
                    self.resources_held.append(resource)
                    resource.is_available = False
                    logging.info(f"Worker {self.name} acquired {resource.name}")
                else:
                    acquired_all = False
                    logging.info(f"Worker {self.name} timed out on {resource.name}; rolling back")
                    break

            if acquired_all:
                return True

            
            self._release_held(reverse=True)

            
            time.sleep(0.05 + random.uniform(0.0, 0.15))

        
        return False

    def perform_work(self):
        for resource in self.resources_held:
            
            time.sleep(0.15)
            delta = random.randint(-10, 10)
            resource.value += delta
            logging.info(f"Worker {self.name} modified {resource.name} by {delta} -> {resource.value}")

    def release_resources(self):
        self._release_held(reverse=True)

    def _release_held(self, reverse: bool = False):
        
        items = self.resources_held[::-1] if reverse else self.resources_held
        for resource in items:
            try:
                resource.is_available = True
            finally:
                
                if resource.lock.locked():
                    resource.lock.release()
                    logging.info(f"Worker {self.name} released {resource.name}")
        self.resources_held.clear()

class ResourceManager:
    def __init__(self):
        self.resources: Dict[str, Resource] = {}
        self.workers: List[Worker] = []
        self.is_running = True
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%H:%M:%S'
        )

    def create_resource(self, name: str, initial_value: int = 100):
        self.resources[name] = Resource(name, initial_value)
        logging.info(f"Created resource {name}")

    def create_worker(self, name: str, resource_names: List[str]):
        resources = [self.resources[name] for name in resource_names]
        worker = Worker(name, resources, self)
        self.workers.append(worker)
        return worker

    def start_workers(self):
        for worker in self.workers:
            worker.start()

    def stop_workers(self):
        self.is_running = False
        for worker in self.workers:
            worker.join(timeout=2.0)

    def monitor_resources(self, duration: int = 10):
        start_time = time.time()
        while time.time() - start_time < duration:
            for name, resource in self.resources.items():
                status = "Available" if resource.is_available else "In use"
                logging.info(f"Resource {name}: Value={resource.value}, Status={status}")
            time.sleep(1)

if __name__ == "__main__":
    manager = ResourceManager()

    
    manager.create_resource("CPU", 100)
    manager.create_resource("Memory", 1000)
    manager.create_resource("Disk", 500)

    
    manager.create_worker("ProcessA", ["CPU", "Memory"])
    manager.create_worker("ProcessB", ["Memory", "Disk"])
    manager.create_worker("ProcessC", ["CPU", "Disk"])
    manager.create_worker("ProcessD", ["CPU", "Disk"])
    manager.create_worker("ProcessE", ["Memory", "Disk"])
    manager.create_worker("ProcessF", ["CPU", "Memory"])
    manager.create_worker("ProcessG", ["Disk", "CPU"])

    manager.start_workers()
    manager.monitor_resources(30)
    manager.stop_workers()

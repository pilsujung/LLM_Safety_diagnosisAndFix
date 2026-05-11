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
        super().__init__()
        self.name = name
        
        self.resources = sorted(resources, key=lambda r: r.name)
        self.manager = manager
        self.resources_held = []

    def run(self):
        retry_count = 0
        max_retries = 50  
        while self.manager.is_running and retry_count < max_retries:
            try:
                self.acquire_resources()
                self.perform_work()
                self.release_resources()
                time.sleep(random.uniform(0.1, 0.5))
                retry_count = 0  
            except Exception as e:
                retry_count += 1
                logging.warning(f"Worker {self.name} retry {retry_count}/{max_retries}: {e}")
                time.sleep(0.1)

    def acquire_resources(self):
        self.resources_held.clear()  
        for resource in self.resources:
            logging.debug(f"Worker {self.name} attempting {resource.name}")
            
            if not resource.lock.acquire(timeout=2.0):
                
                self._release_held_resources()
                raise RuntimeError(f"Timeout acquiring {resource.name}")
            self.resources_held.append(resource)
            resource.is_available = False
            logging.debug(f"Worker {self.name} acquired {resource.name}")

    def perform_work(self):
        time.sleep(random.uniform(0.5, 1.5))
        for resource in self.resources_held:
            resource.value += random.randint(-10, 10)
            logging.info(f"Worker {self.name} modified {resource.name} to {resource.value}")

    def release_resources(self):
        self._release_held_resources()

    def _release_held_resources(self):
        """Helper to safely release held resources"""
        for resource in self.resources_held:
            try:
                resource.is_available = True
                resource.lock.release()
                logging.debug(f"Worker {self.name} released {resource.name}")
            except RuntimeError:
                pass  
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
        resources = [self.resources[n] for n in resource_names]
        worker = Worker(name, resources, self)
        self.workers.append(worker)
        return worker

    def start_workers(self):
        logging.info("Starting all workers...")
        for worker in self.workers:
            worker.start()

    def stop_workers(self):
        logging.info("Stopping workers...")
        self.is_running = False
        for worker in self.workers:
            worker.join(timeout=10)

    def monitor_resources(self, duration: int = 30):
        start_time = time.time()
        while time.time() - start_time < duration and self.is_running:
            for name, resource in self.resources.items():
                status = "Available" if resource.is_available else "In use"
                logging.info(f"Resource {name}: Value={resource.value}, Status={status}")
            time.sleep(2)

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
    manager.monitor_resources(45)
    manager.stop_workers()
    logging.info("Simulation completed successfully!")

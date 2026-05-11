import threading
import time
import random
import logging
from typing import Dict, Any
from dataclasses import dataclass
from queue import Queue, Empty
from contextlib import contextmanager


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)

@dataclass
class ProcessingStats:
    total_processes: int = 0
    successful_processes: int = 0
    failed_processes: int = 0
    avg_processing_time: float = 0.0

class DataProcessor:
    def __init__(self, max_retries: int = 3, timeout: float = 3.0):
        self.data: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.processing_queue = Queue()
        self.max_retries = max_retries
        self.timeout = timeout
        self.stats = ProcessingStats()
        self.shutdown_flag = threading.Event()
        self._worker_threads = []

    @contextmanager
    def timed_operation(self, operation_name: str):
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            logging.info(f"{operation_name} completed in {duration:.2f} seconds")

    def initialize_data(self):
        try:
            with self.lock, self.timed_operation("Data initialization"):
                time.sleep(5)
                self.data.update({
                    'key': 'value',
                    'timestamp': time.time(),
                    'initialized_by': threading.current_thread().name
                })
                logging.info("Data initialized successfully")
                self.data_ready.set()
        except Exception as e:
            logging.error(f"Error initializing data: {e}")
            raise

    def process_data(self):

        if not self.data_ready.wait(timeout=self.timeout):
            logging.error("Timeout waiting for data initialization")
            self.stats.failed_processes += 1
            return False
        
        retry_count = 0
        while retry_count < self.max_retries and not self.shutdown_flag.is_set():
            try:
                delay = 2 if random.random() >= 0.1 else 0
                if delay:
                    logging.debug(f"Applying processing delay of {delay} seconds")
                    time.sleep(delay)

                with self.lock, self.timed_operation("Data processing"):
                    if 'key' in self.data:
                        process_id = random.randint(1000, 9999)
                        self.data[f'process_{process_id}'] = {
                            'processed_at': time.time(),
                            'processed_by': threading.current_thread().name
                        }
                        logging.info(f"Processing data: {self.data['key']} (ID: {process_id})")
                        self.stats.successful_processes += 1
                    else:
                        logging.error("Data not ready or corrupted")
                        self.stats.failed_processes += 1

                    return True

            except Exception as e:
                logging.error(f"Error processing data: {e}")
                retry_count += 1
                self.stats.failed_processes += 1
                time.sleep(1)

        return False

    def worker(self):
        while not self.shutdown_flag.is_set():
            try:
                task = self.processing_queue.get(timeout=1)
            except Empty:
                continue

            if task is None:
                break
            try:
                self.process_data()
            finally:
                self.processing_queue.task_done()

    def start_workers(self, num_workers: int = 3):
        for _ in range(num_workers):
            worker_thread = threading.Thread(target=self.worker, name="worker")
            worker_thread.daemon = True
            worker_thread.start()
            self._worker_threads.append(worker_thread)

    def shutdown(self):
        logging.info("Initiating shutdown sequence")
        self.shutdown_flag.set()


        for _ in self._worker_threads:
            self.processing_queue.put(None)


        for thread in self._worker_threads:
            thread.join()

        logging.info("Shutdown complete")
        self._print_stats()

    def _print_stats(self):
        total_time = time.time() - self.stats.start_time if hasattr(self.stats, 'start_time') else 0
        logging.info("\nProcessing Statistics:")
        logging.info(f"Total processes: {self.stats.total_processes}")
        logging.info(f"Successful processes: {self.stats.successful_processes}")
        logging.info(f"Failed processes: {self.stats.failed_processes}")
        logging.info(f"Total running time: {total_time:.2f} seconds")

    def run(self, num_processes: int = 5):
        self.stats.start_time = time.time()
        

        self.start_workers()


        init_thread = threading.Thread(target=self.initialize_data, name="initializer")
        init_thread.start()
        init_thread.join()


        for _ in range(num_processes):
            self.processing_queue.put(True)
            self.stats.total_processes += 1

        self.processing_queue.join()
        self.shutdown()

def main():
    try:
        processor = DataProcessor(max_retries=3, timeout=3.0)
        processor.run(num_processes=5)
    except KeyboardInterrupt:
        logging.info("Received interrupt signal")
        processor.shutdown()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
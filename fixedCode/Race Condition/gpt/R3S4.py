import threading
import time
import random
from pathlib import Path
from typing import List

class UnsafeLogger:
    def __init__(self, filename: str):
        self.filename = filename
        Path(filename).write_text("")

        self._lock = threading.Lock()

    def write_log(self, message: str):


        with self._lock:
            with open(self.filename, 'a', buffering=1) as file:
                file.write(message + "\n")


def worker(logger: UnsafeLogger, worker_id: int, num_messages: int):
    """Worker function that writes multiple log messages"""
    for msg_num in range(num_messages):
        message = f"Worker-{worker_id} Message-{msg_num}: " + "X" * random.randint(3, 10)
        logger.write_log(message)
        time.sleep(random.uniform(0.001, 0.003))

def verify_log_file(filename: str, num_workers: int, messages_per_worker: int) -> tuple:
    expected_messages = set()
    for worker_id in range(num_workers):
        for msg_num in range(messages_per_worker):
            expected_messages.add(f"Worker-{worker_id} Message-{msg_num}")
    found_messages = set()
    duplicates = 0
    corrupted_lines = 0
    with open(filename, 'r') as file:
        lines = file.readlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if not line.startswith("Worker-") or "Message-" not in line:
            corrupted_lines += 1
            continue
        try:
            msg_id = line.split(":")[0].strip()
            if msg_id in found_messages:
                duplicates += 1
            found_messages.add(msg_id)
        except:
            corrupted_lines += 1
    return len(lines), duplicates, corrupted_lines

def main():
    log_file = "concurrent_log.txt"
    num_workers = 3
    messages_per_worker = 5
    expected_total = num_workers * messages_per_worker

    print(f"Starting logging race condition demonstration:")
    print(f"- Number of workers: {num_workers}")
    print(f"- Messages per worker: {messages_per_worker}")
    print(f"- Expected total messages: {expected_total}")
    print("\nExecuting...\n")

    logger = UnsafeLogger(log_file)
    threads: List[threading.Thread] = []

    for worker_id in range(num_workers):
        thread = threading.Thread(target=worker, args=(logger, worker_id, messages_per_worker))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    total_messages, duplicates, corrupted = verify_log_file(log_file, num_workers, messages_per_worker)

    print("\nResults analysis:")
    print(f"- Expected messages: {expected_total}")
    print(f"- Actual messages in log: {total_messages}")
    print(f"- Duplicate messages: {duplicates}")
    print(f"- Corrupted lines: {corrupted}")
    print(f"- Missing/lost messages: {expected_total - (total_messages - duplicates - corrupted)}")

    print("\nExamining log file contents:")
    with open(log_file, 'r') as file:
        print("\nFirst 10 lines of log file:")
        for i, line in enumerate(file):
            if i >= 10:
                break
            print(f"{i+1}. {line.strip()}")

if __name__ == "__main__":
    main()

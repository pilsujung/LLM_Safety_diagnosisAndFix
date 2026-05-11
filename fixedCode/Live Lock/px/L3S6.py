import threading
import time
import random
from enum import Enum
import logging
import math


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DatabaseLivelock')

class LockStatus(Enum):
    UNLOCKED = 0
    LOCKED = 1

class DataItem:
    def __init__(self, item_id):
        self.item_id = item_id
        self.lock_status = LockStatus.UNLOCKED
        self.lock_holder = None
        self.lock = threading.Lock()

    def try_lock(self, transaction_id):
        with self.lock:
            if self.lock_status == LockStatus.UNLOCKED:
                self.lock_status = LockStatus.LOCKED
                self.lock_holder = transaction_id
                logger.info(f"Transaction {transaction_id} acquired lock on data item {self.item_id}")
                return True
            else:
                logger.info(f"Transaction {transaction_id} failed to lock data item {self.item_id} (held by Transaction {self.lock_holder})")
                return False

    def unlock(self, transaction_id):
        with self.lock:
            if self.lock_status == LockStatus.LOCKED and self.lock_holder == transaction_id:
                self.lock_status = LockStatus.UNLOCKED
                prev_holder = self.lock_holder
                self.lock_holder = None
                logger.info(f"Transaction {prev_holder} released lock on data item {self.item_id}")
                return True
            return False

class Transaction(threading.Thread):
    def __init__(self, transaction_id, data_items, lock_sequence, database, max_attempts=20):
        super().__init__(name=f"Transaction-{transaction_id}")
        self.transaction_id = transaction_id
        self.data_items = data_items
        self.lock_sequence = sorted(lock_sequence)
        self.database = database
        self.locked_items = []
        self.completed = False
        self.deadlocked = False
        self.attempts = 0
        self.max_attempts = max_attempts
        self.last_progress_time = time.time()
        self.base_backoff = 0.01

    def run(self):
        logger.info(f"Transaction {self.transaction_id} started (needs: {self.lock_sequence})")

        while not self.completed and self.attempts < self.max_attempts:
            self.attempts += 1


            for item_id in self.locked_items:
                self.database.data_items[item_id].unlock(self.transaction_id)
            self.locked_items = []

            all_locked = True
            for item_id in self.lock_sequence:
                if self.database.data_items[item_id].try_lock(self.transaction_id):
                    self.locked_items.append(item_id)
                    self.last_progress_time = time.time()
                else:
                    all_locked = False
                    break

            if all_locked:
                logger.info(f"Transaction {self.transaction_id} processing data...")
                time.sleep(0.2)

                for item_id in self.locked_items:
                    self.database.data_items[item_id].unlock(self.transaction_id)
                self.locked_items = []
                self.completed = True
                logger.info(f"Transaction {self.transaction_id} completed successfully")
                break
            else:

                backoff = (self.base_backoff * (2 ** (self.attempts - 1)) * 
                          random.uniform(0.5, 1.5))
                logger.info(f"T{self.transaction_id} attempt {self.attempts} backoff {backoff:.3f}s")
                time.sleep(backoff)

        if not self.completed:
            for item_id in self.locked_items:
                self.database.data_items[item_id].unlock(self.transaction_id)
            logger.warning(f"Transaction {self.transaction_id} failed after {self.attempts} attempts")

class Database:
    def __init__(self, num_items=5):
        self.data_items = {i: DataItem(i) for i in range(num_items)}
        self.transactions = []

    def add_transaction(self, transaction_id, lock_sequence):
        transaction = Transaction(transaction_id, self.data_items, lock_sequence, self)
        self.transactions.append(transaction)
        return transaction

    def start_transactions(self):
        for transaction in self.transactions:
            transaction.start()

    def run_simulation(self, timeout=30):
        self.start_transactions()
        for transaction in self.transactions:
            transaction.join(timeout=timeout)

        completed = sum(1 for t in self.transactions if t.completed)
        failed = len(self.transactions) - completed
        logger.info(f"\n=== RESULTS ===")
        logger.info(f"Completed: {completed}/{len(self.transactions)}")
        logger.info(f"Failed: {failed}")
        return completed == len(self.transactions)

def run_simulation():
    db = Database(num_items=5)
    

    db.add_transaction(1, [0, 1, 2])
    db.add_transaction(2, [1, 2, 3])
    db.add_transaction(3, [2, 3, 4])
    db.add_transaction(4, [3, 4, 0])
    db.add_transaction(5, [4, 0, 1])
    
    success = db.run_simulation()
    logger.info(f"SIMULATION {'SUCCESS' if success else 'PARTIAL FAILURE'}[code_execution:1]")

if __name__ == "__main__":
    logger.info("Fixed Database Livelock Simulation")
    logger.info("=====================================")
    run_simulation()

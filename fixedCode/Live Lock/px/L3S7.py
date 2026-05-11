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
logger = logging.getLogger('DatabaseLivelockFixed')

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
                logger.info(f"T{transaction_id} acquired lock on item {self.item_id}")
                return True
            else:
                logger.debug(f"T{transaction_id} failed item {self.item_id} (held by T{self.lock_holder})")
                return False

    def unlock(self, transaction_id):
        with self.lock:
            if self.lock_status == LockStatus.LOCKED and self.lock_holder == transaction_id:
                self.lock_status = LockStatus.UNLOCKED
                prev_holder = self.lock_holder
                self.lock_holder = None
                logger.info(f"T{prev_holder} released item {self.item_id}")
                return True
        return False

class Transaction(threading.Thread):
    def __init__(self, transaction_id, data_items, lock_sequence, database, timeout=10.0):
        super().__init__(name=f"T{transaction_id}")
        self.transaction_id = transaction_id
        self.data_items = data_items
        self.original_sequence = sorted(lock_sequence)
        self.database = database
        self.locked_items = []
        self.completed = False
        self.attempts = 0
        self.max_attempts = 50
        self.start_time = time.time()
        self.backoff_factor = 1.0

    def run(self):
        logger.info(f"T{self.transaction_id} started, needs locks: {self.original_sequence}")
        
        while not self.completed and self.attempts < self.max_attempts and (time.time() - self.start_time) < 10:
            self.attempts += 1
            

            all_locked = True
            temp_locked = []
            for item_id in self.original_sequence:
                if item_id not in self.locked_items:
                    if self.database.data_items[item_id].try_lock(self.transaction_id):
                        temp_locked.append(item_id)
                    else:
                        all_locked = False
                        break
            
            if all_locked:
                self.locked_items.extend(temp_locked)
                logger.info(f"T{self.transaction_id} got ALL locks: {self.locked_items}")

                time.sleep(0.1)

                for item_id in self.locked_items:
                    self.database.data_items[item_id].unlock(self.transaction_id)
                self.locked_items = []
                self.completed = True
                logger.info(f"T{self.transaction_id} COMPLETED SUCCESSFULLY")
            else:

                for item_id in temp_locked:
                    self.database.data_items[item_id].unlock(self.transaction_id)
                self.locked_items = [item for item in self.locked_items if item not in temp_locked]
                

                backoff = self.backoff_factor * random.uniform(0.05, 0.2)
                logger.debug(f"T{self.transaction_id} attempt {self.attempts} backoff {backoff:.3f}s")
                time.sleep(backoff)
                self.backoff_factor *= 1.3

        if not self.completed:

            for item_id in self.locked_items:
                self.database.data_items[item_id].unlock(self.transaction_id)
            logger.warning(f"T{self.transaction_id} FAILED after {self.attempts} attempts")

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
        for transaction in self.transactions:
            transaction.join(timeout=15)

def run_simulation():
    db = Database(num_items=5)
    

    db.add_transaction(1, [0, 1, 2])
    db.add_transaction(2, [1, 2, 3]) 
    db.add_transaction(3, [2, 3, 4])
    db.add_transaction(4, [3, 4, 0])
    db.add_transaction(5, [4, 0, 1])
    
    logger.info("Starting FIXED simulation with resource hierarchy...")
    start_time = time.time()
    db.start_transactions()
    elapsed = time.time() - start_time
    
    completed = sum(1 for t in db.transactions if t.completed)
    logger.info(f"✅ FIXED: {completed}/5 completed in {elapsed:.1f}s")
    logger.info(f"Attempts: {[t.attempts for t in db.transactions]}")

if __name__ == "__main__":
    logger.info("Database Livelock Simulation - FIXED VERSION")
    logger.info("Key fix: GLOBAL RESOURCE ORDERING (0→1→2→3→4)")
    run_simulation()

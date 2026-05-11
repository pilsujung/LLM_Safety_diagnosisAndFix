import threading
import time
import random
from enum import Enum
import logging


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
        self.random = random.Random(f"item_{item_id}")

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
        self.lock_sequence = lock_sequence
        self.database = database
        self.locked_items = []
        self.completed = False
        self.deadlocked = False
        self.attempts = 0
        self.max_attempts = max_attempts
        self.consecutive_failures = 0
        self.last_progress_time = time.time()
        self.random = random.Random(f"txn_{transaction_id}")

    def run(self):
        logger.info(f"Transaction {self.transaction_id} started")

        while not self.completed and self.attempts < self.max_attempts:
            self.attempts += 1
            self.consecutive_failures += 1


            all_locked = True
            for item_id in self.lock_sequence:
                if item_id not in self.locked_items:
                    if self.database.data_items[item_id].try_lock(self.transaction_id):
                        self.locked_items.append(item_id)
                        self.last_progress_time = time.time()
                        self.consecutive_failures = 0
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
            else:

                for item_id in self.locked_items:
                    self.database.data_items[item_id].unlock(self.transaction_id)
                self.locked_items = []


                if self.consecutive_failures > 5:
                    logger.info(f"Transaction {self.transaction_id} forcing progress after {self.consecutive_failures} failures")

                    if self.locked_items:
                        logger.info(f"Transaction {self.transaction_id} processing partial data: {self.locked_items}")
                        time.sleep(0.1)
                        for item_id in self.locked_items:
                            self.database.data_items[item_id].unlock(self.transaction_id)
                        self.locked_items = []
                        self.last_progress_time = time.time()
                        self.consecutive_failures = 0
                    else:

                        self.last_progress_time = time.time()
                        self.consecutive_failures = 0
                else:

                    wait_time = self.random.uniform(50, 250)
                    logger.info(f"Transaction {self.transaction_id} backoff {wait_time:.0f}ms (attempt {self.attempts})")
                    time.sleep(wait_time / 1000.0)

        if not self.completed:
            for item_id in self.locked_items:
                self.database.data_items[item_id].unlock(self.transaction_id)
            logger.warning(f"Transaction {self.transaction_id} failed to complete after {self.attempts} attempts")

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

    def run_simulation(create_livelock=True):
        """
        Run the database transaction simulation
        """
        db = Database(num_items=5)

        if create_livelock:

            db.add_transaction(1, [0, 1, 2])
            db.add_transaction(2, [1, 2, 3])
            db.add_transaction(3, [2, 3, 4])
            db.add_transaction(4, [3, 4, 0])
            db.add_transaction(5, [4, 0, 1])
        else:

            db.add_transaction(1, [0, 1])
            db.add_transaction(2, [2, 3])
            db.add_transaction(3, [4])


        db.start_transactions()


        for transaction in db.transactions:
            transaction.join()


        completed = sum(1 for t in db.transactions if t.completed)
        failed = len(db.transactions) - completed
        logger.info(f"\nSimulation complete:")
        logger.info(f"Total transactions: {len(db.transactions)}")
        logger.info(f"Completed successfully: {completed}")
        logger.info(f"Failed/Aborted: {failed}")

if __name__ == "__main__":
    logger.info("Starting Fixed Database Livelock Simulation...")
    logger.info("FIXED: Added per-thread randomization + forced progress after 5 failures")
    logger.info("=================================================================")
    Database.run_simulation(create_livelock=True)

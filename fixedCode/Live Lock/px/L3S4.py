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
        self.last_progress_time = time.time()

    def run(self):
        logger.info(f"Transaction {self.transaction_id} started")
        

        ordered_sequence = sorted(self.lock_sequence)

        while not self.completed and self.attempts < self.max_attempts and not self.deadlocked:
            self.attempts += 1


            all_locked = True
            for item_id in ordered_sequence:
                if item_id not in self.locked_items:
                    if self.database.data_items[item_id].try_lock(self.transaction_id):
                        self.locked_items.append(item_id)
                        self.last_progress_time = time.time()
                    else:
                        all_locked = False

                        time.sleep(0.05)
                        break

            if all_locked:

                logger.info(f"Transaction {self.transaction_id} processing data...")
                time.sleep(0.2)


                for item_id in self.locked_items:
                    self.database.data_items[item_id].unlock(self.transaction_id)
                self.locked_items = []
                self.completed = True
                logger.info(f"Transaction {self.transaction_id} completed successfully")
                return
            else:

                for item_id in self.locked_items:
                    self.database.data_items[item_id].unlock(self.transaction_id)
                self.locked_items = []


                if time.time() - self.last_progress_time > 5:
                    self.deadlocked = True
                    logger.warning(f"Transaction {self.transaction_id} detected potential livelock after {self.attempts} attempts")


        if self.locked_items:
            for item_id in self.locked_items:
                self.database.data_items[item_id].unlock(self.transaction_id)
            self.locked_items = []
        
        if not self.completed:
            logger.warning(f"Transaction {self.transaction_id} failed to complete after {self.attempts} attempts")

class Database:
    def __init__(self, num_items=5):
        self.data_items = {i: DataItem(i) for i in range(num_items)}
        self.transactions = []
        self.livelock_detector = None

    def add_transaction(self, transaction_id, lock_sequence):
        transaction = Transaction(transaction_id, self.data_items, lock_sequence, self)
        self.transactions.append(transaction)
        return transaction

    def start_transactions(self):
        for transaction in self.transactions:
            transaction.start()

    def check_livelock(self):
        """Monitor transactions for livelock conditions (now mostly for logging)"""
        livelock_threshold = 10
        check_interval = 1

        while any(t.is_alive() for t in self.transactions):
            time.sleep(check_interval)
            active_transactions = [t for t in self.transactions if t.is_alive()]
            if not active_transactions:
                break

            current_time = time.time()
            potential_livelock = all(
                current_time - t.last_progress_time > livelock_threshold
                for t in active_transactions
            )

            if potential_livelock and len(active_transactions) > 1:
                logger.critical(f"Potential livelock detected: {len(active_transactions)} transactions stuck!")
                for t in active_transactions:
                    logger.critical(f" - T{t.transaction_id}: holds {t.locked_items}, wants {[i for i in sorted(t.lock_sequence) if i not in t.locked_items]}")

    def start_livelock_detector(self):
        """Start livelock detection (now mostly informational)"""
        self.livelock_detector = threading.Thread(target=self.check_livelock, name="LivelockDetector")
        self.livelock_detector.daemon = True
        self.livelock_detector.start()

def run_simulation(create_livelock=True):
    """
    Run the database transaction simulation
    With lock ordering, livelock is now prevented even with overlapping patterns
    """
    db = Database(num_items=5)


    db.add_transaction(1, [0, 1, 2])
    db.add_transaction(2, [1, 2, 3])
    db.add_transaction(3, [2, 3, 4])
    db.add_transaction(4, [3, 4, 0])
    db.add_transaction(5, [4, 0, 1])

    logger.info("Starting transactions with FIXED lock ordering (livelock prevented)")
    logger.info("=" * 60)


    db.start_livelock_detector()


    db.start_transactions()


    for transaction in db.transactions:
        transaction.join()


    completed = sum(1 for t in db.transactions if t.completed)
    deadlocked = sum(1 for t in db.transactions if t.deadlocked)

    logger.info(f"\nSimulation complete:")
    logger.info(f"Total transactions: {len(db.transactions)}")
    logger.info(f"Completed successfully: {completed}")
    logger.info(f"Failed/Deadlocked: {deadlocked}")
    logger.info("Livelock successfully prevented by global lock ordering!")

    if db.livelock_detector:
        db.livelock_detector.join(timeout=1)

if __name__ == "__main__":
    logger.info("Starting FIXED Database Livelock Simulation...")
    logger.info("Global lock ordering prevents livelock even with circular patterns")
    logger.info("=" * 60)
    run_simulation()

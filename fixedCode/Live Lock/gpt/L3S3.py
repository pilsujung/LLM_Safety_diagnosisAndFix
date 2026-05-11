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
        self._mutex = threading.Lock()
        self.lock_status = LockStatus.UNLOCKED
        self.lock_holder = None
        self._meta_lock = threading.Lock()

    def acquire(self, transaction_id, timeout=None):
        """Blocking acquire with optional timeout (None -> block until available)."""
        ok = self._mutex.acquire(timeout=timeout) if timeout is not None else self._mutex.acquire()
        with self._meta_lock:
            if ok:
                self.lock_status = LockStatus.LOCKED
                self.lock_holder = transaction_id
                logger.info(f"Transaction {transaction_id} acquired lock on data item {self.item_id}")
            else:
                logger.info(
                    f"Transaction {transaction_id} timed out waiting for data item {self.item_id} "
                    f"(held by Transaction {self.lock_holder})"
                )
        return ok

    def release(self, transaction_id):
        with self._meta_lock:
            if self.lock_status == LockStatus.LOCKED and self.lock_holder == transaction_id:
                self.lock_status = LockStatus.UNLOCKED
                prev_holder = self.lock_holder
                self.lock_holder = None
                self._mutex.release()
                logger.info(f"Transaction {prev_holder} released lock on data item {self.item_id}")
                return True
            return False

class Transaction(threading.Thread):
    def __init__(self, transaction_id, data_items, lock_sequence, database, max_attempts=1, lock_timeout=None):
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
        self.lock_timeout = lock_timeout

    def _release_all(self):

        for item_id in reversed(self.locked_items):
            self.data_items[item_id].release(self.transaction_id)
        self.locked_items.clear()

    def run(self):
        logger.info(f"Transaction {self.transaction_id} started")

        while not self.completed and self.attempts < self.max_attempts:
            self.attempts += 1
            all_locked = True

            for item_id in self.lock_sequence:
                if item_id in self.locked_items:
                    continue

                if self.data_items[item_id].acquire(self.transaction_id, timeout=self.lock_timeout):
                    self.locked_items.append(item_id)
                    self.last_progress_time = time.time()
                else:

                    all_locked = False
                    break

            if all_locked:

                logger.info(f"Transaction {self.transaction_id} processing data...")
                time.sleep(0.2)


                self._release_all()
                self.completed = True
                logger.info(f"Transaction {self.transaction_id} completed successfully")
            else:

                self._release_all()

                time.sleep(random.uniform(0.01, 0.05))


        if not self.completed:
            self._release_all()
            if self.attempts >= self.max_attempts:
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
        """Monitor transactions for livelock conditions (should not trigger with ordered locking)."""
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

                logger.critical(f"LIVELOCK DETECTED: {len(active_transactions)} transactions are stuck!")
                for t in active_transactions:
                    logger.critical(
                        f"  - Transaction {t.transaction_id} holding: {t.locked_items}, waiting for: "
                        f"{[i for i in t.lock_sequence if i not in t.locked_items]}"
                    )

                break

    def start_livelock_detector(self):
        self.livelock_detector = threading.Thread(target=self.check_livelock, name="LivelockDetector", daemon=True)
        self.livelock_detector.start()

def run_simulation(create_livelock=True):
    """
    Run the database transaction simulation

    Args:
        create_livelock: If True, creates overlapping patterns (safe with ordered locking).
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

    db.start_livelock_detector()
    db.start_transactions()

    for transaction in db.transactions:
        transaction.join()

    completed = sum(1 for t in db.transactions if t.completed)
    deadlocked = sum(1 for t in db.transactions if t.deadlocked)

    logger.info("\nSimulation complete:")
    logger.info(f"Total transactions: {len(db.transactions)}")
    logger.info(f"Completed successfully: {completed}")
    logger.info(f"Potentially livelocked: {deadlocked}")

    if db.livelock_detector:
        db.livelock_detector.join(timeout=1)

if __name__ == "__main__":
    logger.info("Starting Database Livelock Simulation...")
    logger.info("This simulation uses global lock ordering + blocking to avoid livelock/deadlock")
    logger.info("=================================================================")
    run_simulation(create_livelock=True)

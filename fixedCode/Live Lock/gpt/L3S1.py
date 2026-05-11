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

    def try_lock(self, transaction_id) -> bool:
        """Non-blocking attempt to acquire this data item's lock metadata."""
        with self.lock:
            if self.lock_status == LockStatus.UNLOCKED:
                self.lock_status = LockStatus.LOCKED
                self.lock_holder = transaction_id
                logger.info(f"Transaction {transaction_id} acquired lock on data item {self.item_id}")
                return True
            else:
                logger.info(
                    f"Transaction {transaction_id} failed to lock data item {self.item_id} "
                    f"(held by Transaction {self.lock_holder})"
                )
                return False

    def unlock(self, transaction_id) -> bool:
        """Release this data item's lock metadata if held by transaction_id."""
        with self.lock:
            if self.lock_status == LockStatus.LOCKED and self.lock_holder == transaction_id:
                prev_holder = self.lock_holder
                self.lock_status = LockStatus.UNLOCKED
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
        self.aborted = False
        self.attempts = 0
        self.max_attempts = max_attempts
        self.last_progress_time = time.time()

    def _release_all(self):
        """Release all locks currently held by this transaction (in reverse order for clarity)."""
        for item_id in reversed(self.locked_items):
            self.database.data_items[item_id].unlock(self.transaction_id)
        self.locked_items.clear()

    def run(self):
        logger.info(f"Transaction {self.transaction_id} started")

        base_backoff = 0.05
        while (not self.completed) and (not self.aborted) and (self.attempts < self.max_attempts):
            self.attempts += 1


            acquired_all = True
            for item_id in self.lock_sequence:
                if self.aborted:
                    acquired_all = False
                    break
                if item_id not in self.locked_items:
                    if self.data_items[item_id].try_lock(self.transaction_id):
                        self.locked_items.append(item_id)
                        self.last_progress_time = time.time()
                    else:
                        acquired_all = False
                        break

            if self.aborted:

                break

            if acquired_all:

                logger.info(f"Transaction {self.transaction_id} processing data...")
                time.sleep(0.2)
                self._release_all()
                self.completed = True
                logger.info(f"Transaction {self.transaction_id} completed successfully")
            else:

                if self.locked_items:
                    logger.info(f"Transaction {self.transaction_id} releasing partial locks and retrying")
                self._release_all()


                exp = min(self.attempts, 8)
                sleep_s = base_backoff * (2 ** (exp - 1))
                sleep_s = random.uniform(sleep_s * 0.5, sleep_s * 1.5)
                time.sleep(sleep_s)


                self.last_progress_time = time.time()


        self._release_all()

        if self.aborted:
            logger.warning(f"Transaction {self.transaction_id} was aborted by livelock resolver")
        elif not self.completed:
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
        """Monitor transactions for livelock-like stalls and resolve fairly."""
        livelock_threshold = 10
        check_interval = 0.5

        while any(t.is_alive() for t in self.transactions):
            time.sleep(check_interval)
            active = [t for t in self.transactions if t.is_alive()]

            if not active:
                break

            now = time.time()
            stalled = [t for t in active if now - t.last_progress_time > livelock_threshold]


            if len(stalled) >= 2:
                logger.critical(f"LIVELOCK DETECTED: {len(stalled)} transactions appear stalled!")
                for t in stalled:
                    waiting_for = [i for i in t.lock_sequence if i not in t.locked_items]
                    logger.critical(
                        f"  - Transaction {t.transaction_id} holding: {t.locked_items}, waiting for: {waiting_for}"
                    )


                victim = random.choice(stalled)
                logger.warning(f"Resolving livelock by aborting Transaction {victim.transaction_id}")


                victim.aborted = True
                for item_id in list(victim.locked_items):
                    self.data_items[item_id].unlock(victim.transaction_id)
                victim.locked_items.clear()

    def start_livelock_detector(self):
        """Start livelock detection in a separate thread (daemon)."""
        self.livelock_detector = threading.Thread(
            target=self.check_livelock, name="LivelockDetector", daemon=True
        )
        self.livelock_detector.start()


def run_simulation(create_livelock=True):
    """
    Run the database transaction simulation.

    Args:
        create_livelock: If True, creates overlapping lock patterns (now safe due to ordering).
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


    for t in db.transactions:
        t.join()


    completed = sum(1 for t in db.transactions if t.completed)
    aborted = sum(1 for t in db.transactions if t.aborted)

    logger.info("\nSimulation complete:")
    logger.info(f"Total transactions: {len(db.transactions)}")
    logger.info(f"Completed successfully: {completed}")
    logger.info(f"Aborted by resolver: {aborted}")

    if db.livelock_detector:
        db.livelock_detector.join(timeout=1)


if __name__ == "__main__":
    logger.info("Starting Database Livelock Simulation...")
    logger.info("This run uses global lock ordering + two-phase acquisition to avoid livelock.")
    logger.info("=================================================================")
    run_simulation(create_livelock=True)

import threading
import time
import random
from enum import Enum
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DatabaseLivelock")


BACKOFF_MIN_SECONDS = 0.05
BACKOFF_MAX_SECONDS = 0.20


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
                logger.info(
                    "Transaction %s acquired lock on data item %s",
                    transaction_id,
                    self.item_id,
                )
                return True
            else:
                logger.info(
                    "Transaction %s failed to lock data item %s (held by Transaction %s)",
                    transaction_id,
                    self.item_id,
                    self.lock_holder,
                )
                return False

    def unlock(self, transaction_id):
        with self.lock:
            if self.lock_status == LockStatus.LOCKED and self.lock_holder == transaction_id:
                self.lock_status = LockStatus.UNLOCKED
                prev_holder = self.lock_holder
                self.lock_holder = None
                logger.info(
                    "Transaction %s released lock on data item %s",
                    prev_holder,
                    self.item_id,
                )
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

    def _release_all_locks(self):
        """Release all locks currently held by this transaction."""
        if self.locked_items:
            logger.info(
                "Transaction %s releasing partial locks %s",
                self.transaction_id,
                self.locked_items,
            )
        for item_id in self.locked_items:
            self.database.data_items[item_id].unlock(self.transaction_id)
        self.locked_items = []

    def _random_backoff(self, reason: str):
        """Randomized backoff to break lock-acquisition symmetry (livelock avoidance)."""
        wait_time = random.uniform(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
        logger.info(
            "Transaction %s backing off for %.3f seconds (%s)",
            self.transaction_id,
            wait_time,
            reason,
        )
        time.sleep(wait_time)

    def run(self):
        logger.info("Transaction %s started", self.transaction_id)

        while not self.completed and self.attempts < self.max_attempts:

            if self.deadlocked:
                logger.warning(
                    "Transaction %s detected as livelock victim. Aborting.",
                    self.transaction_id,
                )
                break

            self.attempts += 1
            all_locked = True





            for item_id in self.lock_sequence:
                if item_id in self.locked_items:
                    continue

                if self.database.data_items[item_id].try_lock(self.transaction_id):
                    self.locked_items.append(item_id)
                    self.last_progress_time = time.time()
                else:
                    all_locked = False

                    self._release_all_locks()

                    self._random_backoff("lock contention")
                    break

            if all_locked and len(self.locked_items) == len(self.lock_sequence):

                logger.info("Transaction %s processing data...", self.transaction_id)
                time.sleep(0.2)


                self._release_all_locks()
                self.completed = True
                logger.info("Transaction %s completed successfully", self.transaction_id)
            else:

                if time.time() - self.last_progress_time > 5:
                    logger.warning(
                        "Transaction %s has made no progress for a while; marking as deadlocked candidate",
                        self.transaction_id,
                    )


                    self.deadlocked = True


        if not self.completed:
            self._release_all_locks()
            logger.warning(
                "Transaction %s failed to complete after %s attempts",
                self.transaction_id,
                self.attempts,
            )


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
        """Monitor transactions for livelock conditions."""
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
                logger.critical(
                    "LIVELOCK DETECTED: %s transactions appear to be stuck!",
                    len(active_transactions),
                )
                logger.critical("Transactions involved in detected livelock:")
                for t in active_transactions:
                    logger.critical(
                        "  - Transaction %s holding: %s, waiting for: %s",
                        t.transaction_id,
                        t.locked_items,
                        [i for i in t.lock_sequence if i not in t.locked_items],
                    )



                victim = random.choice(active_transactions)
                logger.warning(
                    "Resolving livelock by aborting Transaction %s",
                    victim.transaction_id,
                )
                victim.deadlocked = True


    def start_livelock_detector(self):
        """Start livelock detection in a separate thread."""
        self.livelock_detector = threading.Thread(
            target=self.check_livelock, name="LivelockDetector", daemon=True
        )
        self.livelock_detector.start()


def run_simulation(create_livelock=True):
    """
    Run the database transaction simulation.

    Args:
        create_livelock: If True, creates a lock pattern likely to cause livelock.
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

    logger.info("Simulation complete:")
    logger.info("Total transactions: %s", len(db.transactions))
    logger.info("Completed successfully: %s", completed)
    logger.info("Aborted due to livelock/deadlock: %s", deadlocked)

    if db.livelock_detector:
        db.livelock_detector.join(timeout=1)


if __name__ == "__main__":
    logger.info("Starting Database Livelock Simulation...")
    logger.info("This simulation demonstrates how transactions can get into a livelock situation")
    logger.info("=================================================================")

    run_simulation(create_livelock=True)

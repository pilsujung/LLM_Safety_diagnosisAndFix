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
                logger.info(
                    f"Transaction {transaction_id} failed to lock data item {self.item_id} "
                    f"(held by Transaction {self.lock_holder})"
                )
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
    
    def _release_all_locks(self):
        """Helper used when backing off or aborting."""
        for item_id in self.locked_items:
            self.database.data_items[item_id].unlock(self.transaction_id)
        if self.locked_items:
            logger.info(f"Transaction {self.transaction_id} released all locks and will retry")
        self.locked_items = []

    def run(self):
        logger.info(f"Transaction {self.transaction_id} started")
        

        while not self.completed and not self.deadlocked and self.attempts < self.max_attempts:
            self.attempts += 1
            

            all_locked = True
            for item_id in self.lock_sequence:
                if item_id not in self.locked_items:
                    if self.database.data_items[item_id].try_lock(self.transaction_id):
                        self.locked_items.append(item_id)
                        self.last_progress_time = time.time()
                    else:



                        all_locked = False
                        self._release_all_locks()
                        

                        base = random.uniform(0.1, 0.5)
                        backoff = min(base * (1 + self.attempts / 4.0), 2.0)
                        logger.info(
                            f"Transaction {self.transaction_id} backing off for "
                            f"{backoff:.3f}s before retrying (attempt {self.attempts})"
                        )
                        time.sleep(backoff)
                        break
            
            if self.deadlocked:

                break

            if all_locked and not self.deadlocked:

                logger.info(f"Transaction {self.transaction_id} processing data...")
                time.sleep(0.2)
                

                self._release_all_locks()
                self.completed = True
                logger.info(f"Transaction {self.transaction_id} completed successfully")
        

        if not self.completed:
            self._release_all_locks()
            if self.deadlocked:
                logger.warning(f"Transaction {self.transaction_id} aborted due to livelock resolution")
            else:
                logger.warning(
                    f"Transaction {self.transaction_id} failed to complete after {self.attempts} attempts"
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
        """Monitor transactions for livelock conditions"""
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
                logger.critical("Transactions involved in livelock:")
                for t in active_transactions:
                    logger.critical(
                        f"  - Transaction {t.transaction_id} holding: {t.locked_items}, waiting for: "
                        f"{[i for i in t.lock_sequence if i not in t.locked_items]}"
                    )
                

                victim = random.choice(active_transactions)
                logger.warning(f"Resolving livelock by aborting Transaction {victim.transaction_id}")
                victim.deadlocked = True
                

                for item_id in victim.locked_items:
                    self.data_items[item_id].unlock(victim.transaction_id)
                victim.locked_items = []

    def start_livelock_detector(self):
        """Start livelock detection in a separate thread"""
        self.livelock_detector = threading.Thread(target=self.check_livelock, name="LivelockDetector")
        self.livelock_detector.daemon = True
        self.livelock_detector.start()

def run_simulation(create_livelock=True):
    """
    Run the database transaction simulation
    
    Args:
        create_livelock: If True, creates a lock pattern likely to cause livelock.
                         Our Transaction implementation + detector will resolve it.
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
    logger.info(f"Aborted by livelock resolver: {deadlocked}")
    
    if db.livelock_detector:
        db.livelock_detector.join(timeout=1)

if __name__ == "__main__":
    logger.info("Starting Database Livelock Simulation...")
    logger.info("This simulation demonstrates how livelock can be mitigated with backoff and abort")
    logger.info("=================================================================")
    
    run_simulation(create_livelock=True)

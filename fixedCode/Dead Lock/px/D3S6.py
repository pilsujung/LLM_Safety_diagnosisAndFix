import threading
import time
import logging
from typing import Dict, Optional, Set, Tuple, List

logging.basicConfig(level=logging.INFO, format="%(message)s")

class DatabaseRecord:
    def __init__(self, record_id: int):
        self.record_id = record_id
        self.lock = threading.Lock()
        self.value = 0

class TransactionManager:
    def __init__(self, records_count: int):
        self.records = [DatabaseRecord(i) for i in range(records_count)]
        self._mu = threading.RLock()
        self.owner_by_record: Dict[int, Optional[int]] = {i: None for i in range(records_count)}
        self.wait_for: Dict[int, Set[int]] = {}

    def set_owner(self, record_id: int, tx_id: int) -> None:
        with self._mu:
            self.owner_by_record[record_id] = tx_id

    def get_owner(self, record_id: int) -> Optional[int]:
        with self._mu:
            return self.owner_by_record.get(record_id)

    def add_wait_edge(self, waiter_tx: int, holder_tx: int) -> None:
        if waiter_tx == holder_tx:
            return
        with self._mu:
            self.wait_for.setdefault(waiter_tx, set()).add(holder_tx)

    def clear_tx(self, tx_id: int) -> None:
        with self._mu:
            for record_id in list(self.owner_by_record):
                if self.owner_by_record[record_id] == tx_id:
                    self.owner_by_record[record_id] = None
            self.wait_for.pop(tx_id, None)

    def _detect_cycle_from(self, start_tx: int) -> Optional[Tuple[int, ...]]:
        with self._mu:
            graph = {k: set(v) for k, v in self.wait_for.items()}
            visited: Set[int] = set()
            in_stack: Set[int] = set()
            stack: List[int] = []

            def dfs(u: int) -> Optional[Tuple[int, ...]]:
                visited.add(u)
                in_stack.add(u)
                stack.append(u)
                for v in graph.get(u, set()):
                    if v not in visited:
                        cyc = dfs(v)
                        if cyc:
                            return cyc
                    elif v in in_stack:
                        idx = stack.index(v)
                        return tuple(stack[idx:] + [v])
                stack.pop()
                in_stack.remove(u)
                return None
            return dfs(start_tx)

class DeadlockingTransaction(threading.Thread):
    def __init__(self, tx_id: int, manager: TransactionManager, first_record_id: int,
                 second_record_id: int, barrier_after_first_lock: threading.Barrier,
                 barrier_after_wait_edge: threading.Barrier):
        super().__init__()
        self.tx_id = tx_id
        self.manager = manager
        self.first_record_id = first_record_id
        self.second_record_id = second_record_id
        self.barrier_after_first_lock = barrier_after_first_lock
        self.barrier_after_wait_edge = barrier_after_wait_edge
        self.held_locks: List[threading.Lock] = []

    def abort_transaction(self):
        logging.info(f"T{self.tx_id} aborting transaction")
        for lock in self.held_locks:
            try:
                lock.release()
            except RuntimeError:
                pass  
        self.manager.clear_tx(self.tx_id)
        self.held_locks.clear()

    def run(self):
        first = self.manager.records[self.first_record_id]
        second = self.manager.records[self.second_record_id]

        logging.info(f"T{self.tx_id} acquiring FIRST lock: record {first.record_id}")
        first.lock.acquire()
        self.held_locks.append(first.lock)
        self.manager.set_owner(first.record_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired FIRST lock: record {first.record_id}")

        self.barrier_after_first_lock.wait()

        owner_of_second = self.manager.get_owner(second.record_id)
        if owner_of_second and owner_of_second != self.tx_id:
            self.manager.add_wait_edge(self.tx_id, owner_of_second)
            logging.info(f"T{self.tx_id} will wait for record {second.record_id} owned by T{owner_of_second}")

        self.barrier_after_wait_edge.wait()

        cycle = self.manager._detect_cycle_from(self.tx_id)
        if cycle:
            logging.info(f"[DEADLOCK DETECTED] cycle = {' -> '.join('T'+str(x) for x in cycle)}")
            self.abort_transaction()
            return

        logging.info(f"T{self.tx_id} attempting SECOND lock: record {second.record_id}")
        if second.lock.acquire(timeout=2.0):
            logging.info(f"T{self.tx_id} acquired SECOND lock")
            second.lock.release()
            self.held_locks.pop()  
        else:
            logging.info(f"T{self.tx_id} TIMEOUT on SECOND lock - aborting")
            self.abort_transaction()

def simulate_hard_deadlock_with_notification():
    manager = TransactionManager(2)
    barrier1 = threading.Barrier(2)
    barrier2 = threading.Barrier(2)

    t1 = DeadlockingTransaction(1, manager, 0, 1, barrier1, barrier2)
    t2 = DeadlockingTransaction(2, manager, 1, 0, barrier1, barrier2)

    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)
    logging.info("Simulation completed successfully")

if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()

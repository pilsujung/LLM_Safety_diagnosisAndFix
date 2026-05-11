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

    def clear_owner(self, record_id: int) -> None:
        with self._mu:
            self.owner_by_record[record_id] = None

    def get_owner(self, record_id: int) -> Optional[int]:
        with self._mu:
            return self.owner_by_record.get(record_id)

    def add_wait_edge(self, waiter_tx: int, holder_tx: int) -> None:
        if waiter_tx == holder_tx:
            return
        with self._mu:
            self.wait_for.setdefault(waiter_tx, set()).add(holder_tx)

    def remove_wait_edges(self, tx_id: int) -> None:
        with self._mu:
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
                in_stack.discard(u)
                return None
            return dfs(start_tx)

class DeadlockingTransaction(threading.Thread):
    def __init__(self, tx_id: int, manager: TransactionManager, first_record_id: int, 
                 second_record_id: int, barrier1: threading.Barrier, barrier2: threading.Barrier):
        super().__init__()
        self.tx_id = tx_id
        self.manager = manager
        self.first_record_id = first_record_id
        self.second_record_id = second_record_id
        self.barrier1 = barrier1
        self.barrier2 = barrier2
        self.first_lock_held = False

    def run(self) -> None:
        first = self.manager.records[self.first_record_id]
        second = self.manager.records[self.second_record_id]

        
        logging.info(f"T{self.tx_id} acquiring FIRST lock: record {first.record_id}")
        if first.lock.acquire(timeout=5.0):
            self.first_lock_held = True
            self.manager.set_owner(first.record_id, self.tx_id)
            logging.info(f"T{self.tx_id} acquired FIRST lock: record {first.record_id}")
        else:
            logging.info(f"T{self.tx_id} timeout on first lock")
            return

        self.barrier1.wait()

        
        owner = self.manager.get_owner(self.second_record_id)
        if owner and owner != self.tx_id:
            self.manager.add_wait_edge(self.tx_id, owner)
            logging.info(f"T{self.tx_id} added wait edge for T{owner} (record {self.second_record_id})")

        self.barrier2.wait()

        
        cycle = self.manager._detect_cycle_from(self.tx_id)
        if cycle:
            logging.info(f"[DEADLOCK DETECTED] T{self.tx_id} sees cycle: {' -> '.join(map(str, cycle))}")
            self.abort()
            return

        
        logging.info(f"T{self.tx_id} attempting SECOND lock: record {self.second_record_id}")
        if second.lock.acquire(timeout=3.0):
            logging.info(f"T{self.tx_id} SUCCESS: acquired SECOND lock")
            second.lock.release()
        else:
            logging.info(f"T{self.tx_id} SECOND lock TIMEOUT -> aborting")
            self.abort()

    def abort(self):
        if self.first_lock_held:
            first = self.manager.records[self.first_record_id]
            first.lock.release()
            self.manager.clear_owner(self.first_record_id)
            self.manager.remove_wait_edges(self.tx_id)
            self.first_lock_held = False
            logging.info(f"T{self.tx_id} ABORTED: released locks and cleared state")

def simulate_deadlock_resolution():
    manager = TransactionManager(2)
    barrier1 = threading.Barrier(2)
    barrier2 = threading.Barrier(2)

    t1 = DeadlockingTransaction(1, manager, 0, 1, barrier1, barrier2)
    t2 = DeadlockingTransaction(2, manager, 1, 0, barrier1, barrier2)

    t1.start()
    t2.start()

    t1.join(timeout=15.0)
    t2.join(timeout=15.0)
    logging.info("SIMULATION COMPLETED - no hang!")

if __name__ == "__main__":
    simulate_deadlock_resolution()

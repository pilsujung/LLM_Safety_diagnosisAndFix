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
    """
    Tracks:
      - lock ownership by record_id
      - wait-for graph between transactions (waiter -> holder)
    Detects deadlock when the wait-for graph forms a cycle.
    """

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

    def _detect_cycle_from(self, start_tx: int) -> Optional[Tuple[int, ...]]:
        """
        DFS cycle detection. Returns a cycle path like (T1, T2, T1) if found.
        """
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
                    if cyc is not None:
                        return cyc
                elif v in in_stack:
                    idx = stack.index(v)
                    cycle_path = stack[idx:] + [v]
                    return tuple(cycle_path)

            stack.pop()
            in_stack.remove(u)
            return None

        return dfs(start_tx)


class DeadlockResolvedTransaction(threading.Thread):
    """
    Fixed version: Always acquire locks in ascending order of record_id.
    This prevents circular wait and eliminates deadlock.
    """

    def __init__(
        self,
        tx_id: int,
        manager: TransactionManager,
        first_record_id: int,
        second_record_id: int,
        barrier_after_first_lock: threading.Barrier,
        barrier_after_wait_edge: threading.Barrier,
    ):
        super().__init__()
        self.tx_id = tx_id
        self.manager = manager
        self.first_record_id = first_record_id
        self.second_record_id = second_record_id
        self.barrier_after_first_lock = barrier_after_first_lock
        self.barrier_after_wait_edge = barrier_after_wait_edge

    def run(self) -> None:
        
        if self.first_record_id < self.second_record_id:
            lock_order = [(self.first_record_id, "FIRST"), (self.second_record_id, "SECOND")]
        else:
            lock_order = [(self.second_record_id, "SECOND"), (self.first_record_id, "FIRST")]

        first_lock_id, first_label = lock_order[0]
        second_lock_id, second_label = lock_order[1]

        first_record = self.manager.records[first_lock_id]
        second_record = self.manager.records[second_lock_id]

        logging.info(f"T{self.tx_id} acquiring {first_label} lock: record {first_lock_id}")
        first_record.lock.acquire()
        self.manager.set_owner(first_lock_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired {first_label} lock: record {first_lock_id}")

        
        self.barrier_after_first_lock.wait()

        
        owner_of_second = self.manager.get_owner(second_lock_id)
        if owner_of_second is not None and owner_of_second != self.tx_id:
            self.manager.add_wait_edge(self.tx_id, owner_of_second)
            logging.info(
                f"T{self.tx_id} will wait for record {second_lock_id} owned by T{owner_of_second}"
            )
        else:
            logging.info(
                f"T{self.tx_id} sees record {second_lock_id} as available"
            )

        
        self.barrier_after_wait_edge.wait()

        
        cycle = self.manager._detect_cycle_from(self.tx_id)
        if cycle is not None:
            logging.info(
                f"[DEADLOCK DETECTED] cycle = {' -> '.join('T'+str(x) for x in cycle)}"
            )
        else:
            logging.info(f"T{self.tx_id} - No deadlock detected (as expected)")

        logging.info(
            f"T{self.tx_id} attempting {second_label} lock: record {second_lock_id}"
        )
        second_record.lock.acquire()
        self.manager.set_owner(second_lock_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired {second_label} lock: record {second_lock_id}")

        
        time.sleep(0.1)

        
        logging.info(f"T{self.tx_id} releasing locks")
        second_record.lock.release()
        first_record.lock.release()
        logging.info(f"T{self.tx_id} completed successfully")


def simulate_deadlock_resolved() -> None:
    manager = TransactionManager(records_count=2)

    barrier1 = threading.Barrier(2)
    barrier2 = threading.Barrier(2)

    
    
    t1 = DeadlockResolvedTransaction(
        tx_id=1,
        manager=manager,
        first_record_id=0,
        second_record_id=1,
        barrier_after_first_lock=barrier1,
        barrier_after_wait_edge=barrier2,
    )

    
    
    t2 = DeadlockResolvedTransaction(
        tx_id=2,
        manager=manager,
        first_record_id=1,
        second_record_id=0,
        barrier_after_first_lock=barrier1,
        barrier_after_wait_edge=barrier2,
    )

    t1.start()
    t2.start()

    
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    if t1.is_alive() or t2.is_alive():
        logging.error("Threads did not complete - possible deadlock!")
    else:
        logging.info("All transactions completed successfully - deadlock resolved!")


if __name__ == "__main__":
    simulate_deadlock_resolved()
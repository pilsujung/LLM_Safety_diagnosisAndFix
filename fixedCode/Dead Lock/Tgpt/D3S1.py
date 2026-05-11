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

    def set_owner(self, record_id: int, tx_id: Optional[int]) -> None:
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

    def remove_wait_edges_for(self, tx_id: int) -> None:
        """Remove all wait-for edges from and to tx_id."""
        with self._mu:
            
            self.wait_for.pop(tx_id, None)
            
            for deps in self.wait_for.values():
                deps.discard(tx_id)

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


class DeadlockingTransaction(threading.Thread):
    """
    Original intent:
      - Construct a deterministic deadlock using two records and two transactions.

    Fixed behavior:
      - Still detects a deadlock via wait-for graph.
      - Chooses a deterministic victim transaction.
      - Victim aborts: releases its locks and cleans up wait-for edges.
      - Non-victim proceeds and completes.
      - No thread blocks forever.
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
        first = self.manager.records[self.first_record_id]
        second = self.manager.records[self.second_record_id]

        first_acquired = False
        second_acquired = False

        logging.info(f"T{self.tx_id} acquiring FIRST lock: record {first.record_id}")
        first.lock.acquire()
        first_acquired = True
        self.manager.set_owner(first.record_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired FIRST lock: record {first.record_id}")

        try:
            
            self.barrier_after_first_lock.wait()

            
            owner_of_second = self.manager.get_owner(second.record_id)
            if owner_of_second is not None and owner_of_second != self.tx_id:
                self.manager.add_wait_edge(self.tx_id, owner_of_second)
                logging.info(
                    f"T{self.tx_id} will wait for record {second.record_id} owned by T{owner_of_second}"
                )
            else:
                logging.info(
                    f"T{self.tx_id} sees record {second.record_id} as not owned (unexpected for this demo)"
                )

            
            self.barrier_after_wait_edge.wait()

            cycle = self.manager._detect_cycle_from(self.tx_id)
            victim_tx: Optional[int] = None
            if cycle is not None:
                logging.info(
                    f"[DEADLOCK DETECTED] cycle = {' -> '.join('T'+str(x) for x in cycle)}"
                )
                
                victim_tx = min(cycle)
                logging.info(
                    f"[DEADLOCK RESOLUTION] choosing T{victim_tx} as victim to break the cycle"
                )

            
            if victim_tx == self.tx_id:
                logging.info(
                    f"T{self.tx_id} aborting to resolve deadlock: releasing FIRST lock and not waiting on SECOND"
                )
                return

            logging.info(
                f"T{self.tx_id} attempting SECOND lock: record {second.record_id}"
            )
            
            second_acquired = second.lock.acquire(timeout=2.0)
            if not second_acquired:
                logging.info(
                    f"T{self.tx_id} could not acquire SECOND lock within timeout; aborting"
                )
                return

            self.manager.set_owner(second.record_id, self.tx_id)
            logging.info(
                f"T{self.tx_id} acquired SECOND lock: record {second.record_id}"
            )

            
            time.sleep(0.1)

        finally:
            
            self.manager.remove_wait_edges_for(self.tx_id)

            
            if second_acquired:
                second.lock.release()
                self.manager.set_owner(second.record_id, None)
                logging.info(
                    f"T{self.tx_id} released SECOND lock: record {second.record_id}"
                )

            
            if first_acquired:
                first.lock.release()
                self.manager.set_owner(first.record_id, None)
                logging.info(
                    f"T{self.tx_id} released FIRST lock: record {first.record_id}"
                )


def simulate_hard_deadlock_with_notification() -> None:
    manager = TransactionManager(records_count=2)

    
    barrier1 = threading.Barrier(2)  
    barrier2 = threading.Barrier(2)  

    
    t1 = DeadlockingTransaction(
        tx_id=1,
        manager=manager,
        first_record_id=0,
        second_record_id=1,
        barrier_after_first_lock=barrier1,
        barrier_after_wait_edge=barrier2,
    )

    
    t2 = DeadlockingTransaction(
        tx_id=2,
        manager=manager,
        first_record_id=1,
        second_record_id=0,
        barrier_after_first_lock=barrier1,
        barrier_after_wait_edge=barrier2,
    )

    t1.start()
    t2.start()

    
    t1.join()
    t2.join()


if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()

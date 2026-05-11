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
        """Record that tx_id now owns record_id."""
        with self._mu:
            self.owner_by_record[record_id] = tx_id

    def clear_owner(self, record_id: int, tx_id: int) -> None:
        """Clear owner for record if it's currently held by tx_id."""
        with self._mu:
            if self.owner_by_record.get(record_id) == tx_id:
                self.owner_by_record[record_id] = None

    def get_owner(self, record_id: int) -> Optional[int]:
        with self._mu:
            return self.owner_by_record.get(record_id)

    def add_wait_edge(self, waiter_tx: int, holder_tx: int) -> None:
        """waiter_tx is waiting on holder_tx."""
        if waiter_tx == holder_tx:
            return
        with self._mu:
            self.wait_for.setdefault(waiter_tx, set()).add(holder_tx)

    def remove_all_wait_edges_for(self, tx_id: int) -> None:
        """Remove any wait-for edges where tx_id is waiter or holder."""
        with self._mu:
            
            self.wait_for.pop(tx_id, None)
            
            for waiters in self.wait_for.values():
                waiters.discard(tx_id)

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
    Purpose-built for deterministic deadlock:
      - Each transaction locks its first record.
      - Synchronize so both hold their first locks.
      - Add wait edges for the second lock.
      - Synchronize so both edges exist.
      - Detect and log deadlock cycle.
      - FIXED: choose a victim transaction, abort it, and let the other
        proceed so the program does not hang.
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

        
        self.success = False
        self.aborted = False

    def run(self) -> None:
        first = self.manager.records[self.first_record_id]
        second = self.manager.records[self.second_record_id]

        logging.info(f"T{self.tx_id} acquiring FIRST lock: record {first.record_id}")
        first.lock.acquire()
        self.manager.set_owner(first.record_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired FIRST lock: record {first.record_id}")

        
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
        if cycle is not None:
            logging.info(
                f"[DEADLOCK DETECTED] cycle = {' -> '.join('T'+str(x) for x in cycle)}"
            )
            
            victim = min(cycle)
            if self.tx_id == victim:
                logging.info(f"T{self.tx_id} chosen as deadlock victim, aborting...")
                
                self.manager.remove_all_wait_edges_for(self.tx_id)
                self.manager.clear_owner(first.record_id, self.tx_id)
                
                first.lock.release()
                self.aborted = True
                logging.info(f"T{self.tx_id} ABORTED due to deadlock")
                return
            else:
                logging.info(f"T{self.tx_id} survives deadlock; victim is T{victim}")

        
        logging.info(
            f"T{self.tx_id} attempting SECOND lock: record {second.record_id}"
        )
        second.lock.acquire()
        try:
            logging.info(f"T{self.tx_id} acquired SECOND lock: record {second.record_id}")
            
            time.sleep(0.1)
            self.success = True
            logging.info(f"T{self.tx_id} completing successfully")
        finally:
            
            second.lock.release()
            self.manager.clear_owner(second.record_id, self.tx_id)
            first.lock.release()
            self.manager.clear_owner(first.record_id, self.tx_id)


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

    logging.info("\nFinal status:")
    logging.info(f"T1 success: {t1.success}, aborted: {t1.aborted}")
    logging.info(f"T2 success: {t2.success}, aborted: {t2.aborted}")


if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()

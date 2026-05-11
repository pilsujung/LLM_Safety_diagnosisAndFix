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
    Detects deadlock when the wait-for graph forms a cycle and
    selects a victim to break the cycle.
    """

    def __init__(self, records_count: int):
        self.records = [DatabaseRecord(i) for i in range(records_count)]

        self._mu = threading.RLock()
        self.owner_by_record: Dict[int, Optional[int]] = {i: None for i in range(records_count)}
        self.wait_for: Dict[int, Set[int]] = {}  
        self.deadlock_victim: Optional[int] = None  

    def set_owner(self, record_id: int, tx_id: Optional[int]) -> None:
        """Assign or clear ownership of a record."""
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

    def clear_wait_edges_for(self, tx_id: int) -> None:
        """Remove all wait-for edges involving tx_id (as waiter or holder)."""
        with self._mu:
            
            self.wait_for.pop(tx_id, None)
            
            for waiters in self.wait_for.values():
                waiters.discard(tx_id)

    def _detect_cycle_from(self, start_tx: int) -> Optional[Tuple[int, ...]]:
        """
        DFS cycle detection. Returns a cycle path like (1, 2, 1) if found.
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

    def choose_deadlock_victim(self, cycle: Tuple[int, ...]) -> int:
        """
        Deterministically choose and remember a victim from the cycle.
        Here we pick the smallest tx_id for repeatable behavior.
        """
        with self._mu:
            if self.deadlock_victim is not None:
                
                return self.deadlock_victim

            participants = set(cycle)
            victim = min(participants)
            self.deadlock_victim = victim
            logging.info(f"T{victim} chosen as deadlock victim, aborting...")
            return victim


class DeadlockingTransaction(threading.Thread):
    """
    Demonstrates deadlock detection and resolution:
      - Each transaction locks its first record.
      - Synchronize so both hold their first locks.
      - Add wait edges for the second lock.
      - Synchronize so both edges exist.
      - Detect the deadlock cycle.
      - Choose a victim: victim aborts, survivor proceeds and completes.
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
            victim = self.manager.choose_deadlock_victim(cycle)
        else:
            victim = None

        
        if victim == self.tx_id:
            
            self.manager.clear_wait_edges_for(self.tx_id)
            
            self.manager.set_owner(first.record_id, None)
            first.lock.release()
            logging.info(f"T{self.tx_id} ABORTED due to deadlock")
            return

        
        logging.info(
            f"T{self.tx_id} attempting SECOND lock: record {second.record_id}"
        )
        second.lock.acquire()
        self.manager.set_owner(second.record_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired SECOND lock: record {second.record_id}")
        logging.info(f"T{self.tx_id} completing successfully")

        
        second.lock.release()
        first.lock.release()
        self.manager.set_owner(first.record_id, None)
        self.manager.set_owner(second.record_id, None)


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

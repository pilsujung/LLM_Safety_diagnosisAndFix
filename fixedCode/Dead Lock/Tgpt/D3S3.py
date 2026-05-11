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
    Also supports cooperative deadlock resolution by aborting a chosen victim.
    """

    def __init__(self, records_count: int):
        self.records = [DatabaseRecord(i) for i in range(records_count)]

        self._mu = threading.RLock()
        self.owner_by_record: Dict[int, Optional[int]] = {i: None for i in range(records_count)}
        self.wait_for: Dict[int, Set[int]] = {}  

        
        self._deadlock_victim: Optional[int] = None
        self._aborted: Set[int] = set()

    def set_owner(self, record_id: int, tx_id: int) -> None:
        with self._mu:
            self.owner_by_record[record_id] = tx_id

    def clear_owner(self, record_id: int, tx_id: int) -> None:
        with self._mu:
            if self.owner_by_record.get(record_id) == tx_id:
                self.owner_by_record[record_id] = None

    def get_owner(self, record_id: int) -> Optional[int]:
        with self._mu:
            return self.owner_by_record.get(record_id)

    def add_wait_edge(self, waiter_tx: int, holder_tx: int) -> None:
        if waiter_tx == holder_tx:
            return
        with self._mu:
            self.wait_for.setdefault(waiter_tx, set()).add(holder_tx)

    def remove_tx_from_wait_graph(self, tx_id: int) -> None:
        """Remove tx_id as a waiter and as a dependency of others."""
        with self._mu:
            self.wait_for.pop(tx_id, None)
            for deps in self.wait_for.values():
                deps.discard(tx_id)

    def is_aborted(self, tx_id: int) -> bool:
        with self._mu:
            return tx_id in self._aborted

    def resolve_deadlock(self, cycle: Tuple[int, ...]) -> Tuple[int, bool]:
        """
        Choose exactly one victim deterministically for the whole run.
        Returns (victim_tx_id, chosen_now).
        """
        with self._mu:
            if self._deadlock_victim is not None:
                return self._deadlock_victim, False

            nodes = cycle
            if len(nodes) > 1 and nodes[0] == nodes[-1]:
                nodes = nodes[:-1]  

            
            victim = max(nodes)
            self._deadlock_victim = victim
            self._aborted.add(victim)
            return victim, True

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
    Deterministic deadlock demo + resolution:
      - lock first record
      - synchronize (both hold first lock)
      - add wait edge for second lock
      - synchronize (both edges exist)
      - detect cycle
      - resolve by aborting one victim cooperatively (release + exit)
      - surviving tx acquires second lock and completes
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

    def run(self) -> None:
        first = self.manager.records[self.first_record_id]
        second = self.manager.records[self.second_record_id]

        acquired_first = False
        acquired_second = False

        try:
            logging.info(f"T{self.tx_id} acquiring FIRST lock: record {first.record_id}")
            first.lock.acquire()
            acquired_first = True
            self.manager.set_owner(first.record_id, self.tx_id)
            logging.info(f"T{self.tx_id} acquired FIRST lock: record {first.record_id}")

            
            self.barrier_after_first_lock.wait()

            
            owner_of_second = self.manager.get_owner(second.record_id)
            if owner_of_second is not None and owner_of_second != self.tx_id:
                self.manager.add_wait_edge(self.tx_id, owner_of_second)
                logging.info(
                    f"T{self.tx_id} will wait for record {second.record_id} owned by T{owner_of_second}"
                )

            
            self.barrier_after_wait_edge.wait()

            cycle = self.manager._detect_cycle_from(self.tx_id)
            if cycle is not None:
                logging.info(
                    f"[DEADLOCK DETECTED] cycle = {' -> '.join('T'+str(x) for x in cycle)}"
                )
                victim, chosen_now = self.manager.resolve_deadlock(cycle)
                if chosen_now:
                    logging.info(f"T{victim} chosen as deadlock victim, aborting...")

            
            if self.manager.is_aborted(self.tx_id):
                logging.info(f"T{self.tx_id} ABORTED due to deadlock")
                return

            
            logging.info(f"T{self.tx_id} attempting SECOND lock: record {second.record_id}")
            while True:
                if self.manager.is_aborted(self.tx_id):
                    logging.info(f"T{self.tx_id} ABORTED while waiting for SECOND lock")
                    return
                if second.lock.acquire(timeout=0.05):
                    acquired_second = True
                    break

            self.manager.set_owner(second.record_id, self.tx_id)
            logging.info(f"T{self.tx_id} acquired SECOND lock: record {second.record_id}")

            
            time.sleep(0.01)

            self.success = True
            logging.info(f"T{self.tx_id} completing successfully")

        finally:
            
            self.manager.remove_tx_from_wait_graph(self.tx_id)

            if acquired_second:
                self.manager.clear_owner(second.record_id, self.tx_id)
                second.lock.release()

            if acquired_first:
                self.manager.clear_owner(first.record_id, self.tx_id)
                first.lock.release()


def simulate_deadlock_with_resolution() -> None:
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
    logging.info(f"T1 success: {t1.success}")
    logging.info(f"T2 success: {t2.success}")


if __name__ == "__main__":
    simulate_deadlock_with_resolution()

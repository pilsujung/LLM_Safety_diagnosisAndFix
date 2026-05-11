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
    Detects deadlock when the wait-for graph forms a cycle and chooses a victim.
    """

    def __init__(self, records_count: int):
        self.records = [DatabaseRecord(i) for i in range(records_count)]

        self._mu = threading.RLock()
        self.owner_by_record: Dict[int, Optional[int]] = {
            i: None for i in range(records_count)
        }
        self.wait_for: Dict[int, Set[int]] = {}  
        self._deadlock_victim: Optional[int] = None

    def set_owner(self, record_id: int, tx_id: Optional[int]) -> None:
        """Set or clear (tx_id=None) the owner of a record."""
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
        """Remove all edges coming from or going to tx_id."""
        with self._mu:
            
            self.wait_for.pop(tx_id, None)

            
            empty_keys: List[int] = []
            for k, vs in self.wait_for.items():
                vs.discard(tx_id)
                if not vs:
                    empty_keys.append(k)

            for k in empty_keys:
                self.wait_for.pop(k, None)

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
        Given a cycle, deterministically choose a single victim transaction id.
        The first caller sets the victim; later callers see the same value.
        """
        with self._mu:
            if self._deadlock_victim is None:
                
                unique_txs = list(dict.fromkeys(cycle))
                unique_txs.pop()  
                
                self._deadlock_victim = max(unique_txs)
            return self._deadlock_victim


class DeadlockingTransaction(threading.Thread):
    """
    Purpose-built for deterministic deadlock, but now with resolution:
      - Each transaction locks its first record.
      - Synchronize so both hold their first locks.
      - Add wait-for edges for the second lock.
      - Synchronize so both edges exist.
      - Detect deadlock, choose a victim.
      - Victim aborts (releases first lock); survivor acquires second lock and finishes.
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
            victim = self.manager.choose_deadlock_victim(cycle)
            logging.info(
                f"[DEADLOCK DETECTED] cycle = "
                f"{' -> '.join('T' + str(x) for x in cycle)}; victim = T{victim}"
            )

            if victim == self.tx_id:
                
                logging.info(
                    f"T{self.tx_id} chosen as victim, releasing FIRST lock on "
                    f"record {first.record_id} and aborting"
                )
                first.lock.release()
                self.manager.set_owner(first.record_id, None)
                self.manager.clear_wait_edges_for(self.tx_id)
                return

        
        logging.info(
            f"T{self.tx_id} attempting SECOND lock: record {second.record_id}"
        )
        second.lock.acquire()
        self.manager.set_owner(second.record_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired SECOND lock: record {second.record_id}")

        
        time.sleep(0.1)

        
        second.lock.release()
        self.manager.set_owner(second.record_id, None)
        first.lock.release()
        self.manager.set_owner(first.record_id, None)
        self.manager.clear_wait_edges_for(self.tx_id)
        logging.info(f"T{self.tx_id} finished successfully")


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

    logging.info("Simulation finished (no deadlock)")


if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()

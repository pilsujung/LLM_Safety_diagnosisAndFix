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
    Tracks (kept for compatibility, but no longer used for deadlock demo):
      - lock ownership by record_id
      - wait-for graph between transactions (waiter -> holder)
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
        (Not used anymore in the fixed version.)
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
    FIXED VERSION:
    Instead of intentionally causing a deadlock by locking first_record_id
    and then second_record_id as given, this thread always acquires the two
    record locks in a *global order* (by record_id).

    This matches your Java examples:
      -     (resource/account id )  .
      -   (circular wait)    .
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
        
        rec_a = self.manager.records[self.first_record_id]
        rec_b = self.manager.records[self.second_record_id]

        
        
        if rec_a.record_id <= rec_b.record_id:
            first = rec_a
            second = rec_b
        else:
            first = rec_b
            second = rec_a

        
        if first is second:
            logging.info(f"T{self.tx_id} locking single record {first.record_id}")
            with first.lock:
                logging.info(f"T{self.tx_id} done with record {first.record_id}")
            return

        logging.info(
            f"T{self.tx_id} acquiring FIRST lock (ordered): record {first.record_id}"
        )
        with first.lock:
            logging.info(
                f"T{self.tx_id} acquired FIRST lock (ordered): record {first.record_id}"
            )

            
            time.sleep(0.1)

            logging.info(
                f"T{self.tx_id} acquiring SECOND lock (ordered): record {second.record_id}"
            )
            with second.lock:
                logging.info(
                    f"T{self.tx_id} acquired SECOND lock (ordered): record {second.record_id}"
                )
                
                first.value += 1
                second.value -= 1
                logging.info(
                    f"T{self.tx_id} completed operation between records "
                    f"{first.record_id} and {second.record_id}"
                )

        logging.info(f"T{self.tx_id} finished without deadlock")


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

    print("\nFinal values:")
    for rec in manager.records:
        print(f"record {rec.record_id}: value={rec.value}")


if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()

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
    (The wait-for / owner tracking is now unused for deadlock avoidance,
     because we prevent deadlock via strict lock ordering.)
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
    Deadlock-resolved version:
      - Each transaction still *logically* handles (first_record_id, second_record_id).
      - But the actual lock acquisition order is determined by record_id:
            lower record_id -> higher record_id
        for EVERY thread.
      - This removes the possibility of circular wait (same idea as Java examples).
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

        
        
        if rec_a.record_id < rec_b.record_id:
            first_lock = rec_a
            second_lock = rec_b
        elif rec_a.record_id > rec_b.record_id:
            first_lock = rec_b
            second_lock = rec_a
        else:
            
            logging.info(f"T{self.tx_id} locking single record {rec_a.record_id}")
            with rec_a.lock:
                logging.info(f"T{self.tx_id} in critical section of record {rec_a.record_id}")
                time.sleep(0.1)
            logging.info(f"T{self.tx_id} finished with record {rec_a.record_id}")
            return

        logging.info(
            f"T{self.tx_id} will lock records in order: "
            f"{first_lock.record_id} -> {second_lock.record_id}"
        )

        
        logging.info(f"T{self.tx_id} acquiring lock for record {first_lock.record_id}")
        with first_lock.lock:
            logging.info(f"T{self.tx_id} acquired lock for record {first_lock.record_id}")
            time.sleep(0.1)  

            
            logging.info(f"T{self.tx_id} acquiring lock for record {second_lock.record_id}")
            with second_lock.lock:
                logging.info(f"T{self.tx_id} acquired lock for record {second_lock.record_id}")

                
                first_lock.value += 1
                second_lock.value -= 1

                logging.info(
                    f"T{self.tx_id} completed work on records "
                    f"{first_lock.record_id} and {second_lock.record_id}"
                )

        logging.info(f"T{self.tx_id} finished without deadlock")


def simulate_deadlock_resolved() -> None:
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

    logging.info(
        f"Final values: record[0].value={manager.records[0].value}, "
        f"record[1].value={manager.records[1].value}"
    )


if __name__ == "__main__":
    simulate_deadlock_resolved()

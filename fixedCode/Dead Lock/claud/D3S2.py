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
        self.aborted_txs: Set[int] = set()  

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

    def mark_aborted(self, tx_id: int) -> None:
        with self._mu:
            self.aborted_txs.add(tx_id)

    def is_aborted(self, tx_id: int) -> bool:
        with self._mu:
            return tx_id in self.aborted_txs

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
    Transaction that detects deadlock and aborts if necessary.
    The transaction with the higher ID aborts to break the cycle.
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
            
            
            
            max_tx_in_cycle = max(cycle)
            if self.tx_id == max_tx_in_cycle:
                logging.info(f"T{self.tx_id} ABORTING to break deadlock")
                self.manager.mark_aborted(self.tx_id)
                self.manager.remove_wait_edges(self.tx_id)
                
                
                self.manager.clear_owner(first.record_id)
                first.lock.release()
                logging.info(f"T{self.tx_id} released lock on record {first.record_id}")
                return

        
        if self.manager.is_aborted(self.tx_id):
            logging.info(f"T{self.tx_id} is aborted, exiting")
            return

        logging.info(
            f"T{self.tx_id} attempting SECOND lock: record {second.record_id}"
        )
        second.lock.acquire()
        self.manager.set_owner(second.record_id, self.tx_id)
        logging.info(f"T{self.tx_id} acquired SECOND lock: record {second.record_id}")

        
        logging.info(f"T{self.tx_id} COMMITTED successfully")
        
        
        self.manager.remove_wait_edges(self.tx_id)
        self.manager.clear_owner(first.record_id)
        self.manager.clear_owner(second.record_id)
        first.lock.release()
        second.lock.release()


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
    
    logging.info("=== Program completed successfully ===")


if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()
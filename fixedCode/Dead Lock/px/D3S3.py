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
        self.active_txs: Set[int] = set()

    def register_tx(self, tx_id: int) -> None:
        with self._mu:
            self.active_txs.add(tx_id)

    def unregister_tx(self, tx_id: int) -> None:
        with self._mu:
            self.active_txs.discard(tx_id)
            self.wait_for.pop(tx_id, None)
            
            for rid, owner in list(self.owner_by_record.items()):
                if owner == tx_id:
                    self.owner_by_record[rid] = None

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

    def detect_deadlock(self) -> Optional[Tuple[int, ...]]:
        with self._mu:
            graph = {k: set(v) for k, v in self.wait_for.items()}
            visited: Dict[int, bool] = {}
            in_stack: Dict[int, bool] = {}
            stack: List[int] = []

            def dfs(u: int) -> Optional[Tuple[int, ...]]:
                visited[u] = True
                in_stack[u] = True
                stack.append(u)
                for v in graph.get(u, set()):
                    if v not in visited:
                        cyc = dfs(v)
                        if cyc is not None:
                            return cyc
                    elif v in in_stack:
                        idx = stack.index(v)
                        return tuple(stack[idx:] + [v])
                stack.pop()
                in_stack[u] = False
                return None

            for tx in list(self.active_txs):
                if tx not in visited:
                    cycle = dfs(tx)
                    if cycle:
                        return cycle
            return None

class DeadlockingTransaction(threading.Thread):
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
        self.first_lock = None
        self.second_lock = None

    def run(self) -> None:
        try:
            self.manager.register_tx(self.tx_id)
            first = self.manager.records[self.first_record_id]
            second = self.manager.records[self.second_record_id]

            logging.info(f"T{self.tx_id} acquiring FIRST lock: record {first.record_id}")
            self.first_lock = first.lock.acquire(timeout=5.0)
            if not self.first_lock:
                raise RuntimeError("First lock timeout")
            self.manager.set_owner(first.record_id, self.tx_id)
            logging.info(f"T{self.tx_id} acquired FIRST lock: record {first.record_id}")

            self.barrier_after_first_lock.wait()

            owner_of_second = self.manager.get_owner(second.record_id)
            if owner_of_second is not None and owner_of_second != self.tx_id:
                self.manager.add_wait_edge(self.tx_id, owner_of_second)
                logging.info(f"T{self.tx_id} will wait for record {second.record_id} owned by T{owner_of_second}")

            self.barrier_after_wait_edge.wait()

            
            cycle = self.manager.detect_deadlock()
            if cycle is not None:
                logging.info(f"[DEADLOCK DETECTED] cycle = {' -> '.join('T'+str(x) for x in cycle)}")
                
                raise RuntimeError(f"Deadlock detected, aborting T{self.tx_id}")

            logging.info(f"T{self.tx_id} attempting SECOND lock: record {second.record_id}")
            self.second_lock = second.lock.acquire(timeout=2.0)
            if not self.second_lock:
                raise RuntimeError("Second lock timeout")
            logging.info(f"T{self.tx_id} acquired SECOND lock: record {second.record_id}")

            
            time.sleep(0.1)
            logging.info(f"T{self.tx_id} work done")

        except Exception as e:
            logging.info(f"T{self.tx_id} ABORTED: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        try:
            if self.first_lock:
                self.manager.records[self.first_record_id].lock.release()
                self.manager.set_owner(self.first_record_id, None)
                logging.info(f"T{self.tx_id} released FIRST lock")
            if self.second_lock:
                self.manager.records[self.second_record_id].lock.release()
                self.manager.set_owner(self.second_record_id, None)
                logging.info(f"T{self.tx_id} released SECOND lock")
        except:
            pass
        finally:
            self.manager.unregister_tx(self.tx_id)

def simulate_hard_deadlock_with_notification() -> None:
    manager = TransactionManager(records_count=2)
    barrier1 = threading.Barrier(2)
    barrier2 = threading.Barrier(2)

    t1 = DeadlockingTransaction(1, manager, 0, 1, barrier1, barrier2)
    t2 = DeadlockingTransaction(2, manager, 1, 0, barrier1, barrier2)

    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)
    logging.info("Simulation completed successfully")

if __name__ == "__main__":
    simulate_hard_deadlock_with_notification()

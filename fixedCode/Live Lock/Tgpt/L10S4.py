import threading
import time
import random
from collections import deque
from dataclasses import dataclass

class LivelockDetector:
    """
    Heuristic livelock detector:
    - High activity (many transitions/sec)
    - Repetitive states (few unique states)
    - No "progress" states observed in the window
    """
    def __init__(self, window_s=6.0, min_events=12):
        self.window_s = window_s
        self.min_events = min_events
        self._states = {}
        self._lock = threading.Lock()

    def record(self, name: str, state: str):
        with self._lock:
            dq = self._states.setdefault(name, deque(maxlen=200))
            dq.append((time.time(), state))

    def report(self):
        now = time.time()
        out = {}
        with self._lock:
            for name, dq in self._states.items():
                recent = [(ts, st) for ts, st in dq if now - ts <= self.window_s]
                if len(recent) < self.min_events:
                    continue

                times = [ts for ts, _ in recent]
                seq = [st for _, st in recent]
                uniq = len(set(seq))
                span = max(times) - min(times)
                rate = (len(recent) / span) if span > 0 else 0.0
                progress = sum(1 for st in seq if st == "progress")

                livelocked = (rate > 2.0 and uniq <= 4 and progress == 0)

                out[name] = {
                    "livelocked": livelocked,
                    "events": len(recent),
                    "unique_states": uniq,
                    "rate": round(rate, 2),
                    "tail": seq[-6:],
                }
        return out


def other_active(me: str, peers: list[str], running_flag):

    return running_flag["on"] and any(p != me for p in peers)


def run_for(duration_s: float, stop_flag):
    time.sleep(duration_s)
    stop_flag["on"] = False





def scenario_chat(duration_s=6.0, yield_prob=0.85, delay=0.05):
    det = LivelockDetector(window_s=duration_s, min_events=12)
    lock = threading.Lock()
    queue = []
    stop = {"on": True}
    users = ["User1", "User2"]

    messages = {
        "User1": ["Hello", "How are you", "Anyone there"],
        "User2": ["Hi", "I'm good", "Yes here"],
    }


    MAX_POLITE_STREAK = 3

    def sender(name: str):
        polite_streak = 0
        for m in messages[name]:
            while stop["on"]:
                det.record(name, "try")
                if lock.acquire(False):
                    try:
                        if (other_active(name, users, stop)
                            and random.random() < yield_prob
                            and polite_streak < MAX_POLITE_STREAK):
                            polite_streak += 1
                            det.record(name, "yield")

                            continue


                        polite_streak = 0
                        queue.append((name, m))
                        det.record(name, "progress")
                        break
                    finally:
                        lock.release()
                det.record(name, "wait")
                time.sleep(delay)
        det.record(name, "done")

    ts = [threading.Thread(target=sender, args=(u,), name=u) for u in users]
    for t in ts:
        t.start()
    run_for(duration_s, stop)
    for t in ts:
        t.join(timeout=1)

    return det, {"sent": len(queue), "expected": sum(len(v) for v in messages.values())}





@dataclass
class Item:
    stock: int
    lock: threading.Lock


def scenario_shop(duration_s=6.0, yield_prob=0.85, delay=0.05):
    det = LivelockDetector(window_s=duration_s, min_events=12)
    stop = {"on": True}
    customers = ["Customer1", "Customer2", "Customer3"]

    inv = {
        "laptop": Item(stock=1, lock=threading.Lock()),
        "phone":  Item(stock=1, lock=threading.Lock()),
    }
    purchases = []


    MAX_POLITE_STREAK = 3

    def shopper(name: str):
        for item in ("laptop", "phone"):
            polite_streak = 0
            while stop["on"]:
                det.record(name, "try")
                it = inv[item]
                if it.lock.acquire(False):
                    try:
                        det.record(name, "hold")
                        if (other_active(name, customers, stop)
                            and random.random() < yield_prob
                            and polite_streak < MAX_POLITE_STREAK):
                            polite_streak += 1
                            det.record(name, "yield")
                            continue


                        polite_streak = 0
                        if it.stock > 0:
                            it.stock -= 1
                            purchases.append((name, item))
                            det.record(name, "progress")
                        else:
                            det.record(name, "no_stock")
                        break
                    finally:
                        it.lock.release()

                det.record(name, "wait")
                time.sleep(delay)
        det.record(name, "done")

    ts = [threading.Thread(target=shopper, args=(c,), name=c) for c in customers]
    for t in ts:
        t.start()
    run_for(duration_s, stop)
    for t in ts:
        t.join(timeout=1)

    remaining = {k: v.stock for k, v in inv.items()}
    return det, {"purchases": len(purchases), "remaining": remaining}





@dataclass
class Task:
    name: str
    prio: int


def scenario_tasks(duration_s=6.0, yield_prob=0.85, delay=0.05):
    det = LivelockDetector(window_s=duration_s, min_events=12)
    stop = {"on": True}
    workers = [("Worker1", 3), ("Worker2", 4), ("Worker3", 3)]
    names = [w for w, _ in workers]

    lock = threading.Lock()
    tasks = [
        Task("Critical Bug Fix", 5),
        Task("Docs Update", 3),
        Task("Feature Request", 4),
    ]
    assigned: dict[str, str] = {}
    done = []

    max_skill = max(skill for _, skill in workers)

    MAX_REASSIGN = 2
    yield_counts: dict[tuple[str, str], int] = {}

    def worker(name: str, skill: int):
        nonlocal tasks
        while stop["on"]:

            with lock:
                if not tasks:

                    det.record(name, "done")
                    break


                tasks.sort(key=lambda t: t.prio, reverse=True)
                task = next(
                    (t for t in tasks if assigned.get(t.name) in (None, name)),
                    None,
                )
                if not task:

                    det.record(name, "idle")
                    task = None
                else:
                    assigned[task.name] = name
                    det.record(name, "claim")

            if not stop["on"]:
                break

            if task is None:

                time.sleep(delay * 3)
                continue

            key = (name, task.name)
            cnt = yield_counts.get(key, 0)


            time.sleep(delay)
            more_skilled_exists = any(skill2 > skill for _, skill2 in workers)

            if (other_active(name, names, stop)
                and task.prio > skill
                and more_skilled_exists
                and random.random() < yield_prob
                and cnt < MAX_REASSIGN):
                with lock:
                    if assigned.get(task.name) == name:
                        assigned.pop(task.name, None)
                        det.record(name, "yield")
                        yield_counts[key] = cnt + 1
                continue


            det.record(name, "work")
            time.sleep(delay * 4)
            with lock:
                if assigned.get(task.name) == name and task in tasks:
                    tasks.remove(task)
                    assigned.pop(task.name, None)
                    done.append((name, task.name))
                    det.record(name, "progress")

        det.record(name, "stop")

    ts = [threading.Thread(target=worker, args=w, name=w[0]) for w in workers]
    for t in ts:
        t.start()
    run_for(duration_s, stop)
    for t in ts:
        t.join(timeout=1)

    return det, {"done": len(done), "remaining": len(tasks)}





def print_report(title: str, det: LivelockDetector, summary: dict):
    print("\n" + "=" * 64)
    print(title)
    print("Summary:", summary)

    r = det.report()
    if not r:
        print("No data to analyze.")
        return

    livelocked = [k for k, v in r.items() if v["livelocked"]]
    print("Livelock:", "DETECTED" if livelocked else "not detected")
    for name, v in sorted(r.items()):
        flag = "LIVELOCK" if v["livelocked"] else "OK"
        print(
            f"- {name:10s} [{flag}] "
            f"events={v['events']} uniq={v['unique_states']} "
            f"rate={v['rate']}/s tail={v['tail']}"
        )


def main():
    det, summary = scenario_chat(duration_s=6.0, yield_prob=0.995, delay=0.01)
    print_report("SCENARIO 1: Chat (polite send)", det, summary)

    det, summary = scenario_shop(duration_s=6.0, yield_prob=0.995, delay=0.01)
    print_report("SCENARIO 2: Shop (polite shoppers)", det, summary)

    det, summary = scenario_tasks(duration_s=6.0, yield_prob=0.995, delay=0.01)
    print_report("SCENARIO 3: Tasks (polite reassignment)", det, summary)


if __name__ == "__main__":
    main()

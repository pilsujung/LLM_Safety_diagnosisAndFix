import threading
import time
import random
from dataclasses import dataclass, field
from contextlib import contextmanager

student_record = {
    "student_id": "20251234",
    "name": "Kim Jisoo",
    "gpa": 3.20,
    "address": "Seoul",
    "last_update": None,
}
record_lock = threading.Lock()
audit_lock = threading.Lock()
RUN_SECONDS = 3.0

FIRST_LOCK_HOLD_SECONDS = 0.001
MONITOR_INTERVAL_SECONDS = 0.5
STALL_SECONDS = 1.0
MIN_ATTEMPTS_GROWTH = 200


@dataclass
class Stats:
    attempts: dict = field(default_factory=lambda: {"registrar": 0, "advisor": 0})
    successes: dict = field(default_factory=lambda: {"registrar": 0, "advisor": 0})
    failures: dict = field(default_factory=lambda: {"registrar": 0, "advisor": 0})
    last_commit_time: float = field(default_factory=lambda: time.time())
    lock: threading.Lock = field(default_factory=threading.Lock)

    def inc(self, category: str, who: str) -> None:
        with self.lock:
            getattr(self, category)[who] += 1
            if category == "successes":
                self.last_commit_time = time.time()

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "attempts": dict(self.attempts),
                "successes": dict(self.successes),
                "failures": dict(self.failures),
                "last_commit_time": self.last_commit_time,
            }


@contextmanager
def acquire_locks(*locks):
    """
    Acquire multiple locks in a globally consistent order to avoid deadlock
    and livelock. Any caller can pass the locks in any order; internally we
    sort them by object id so all threads use the same order.
    """
    ordered = sorted(locks, key=lambda l: id(l))
    for lock in ordered:
        lock.acquire()
    try:
        yield
    finally:
        for lock in reversed(ordered):
            lock.release()


def registrar_service_update_grade(stop_event: threading.Event, stats: Stats):
    """
    RegistrarService updates GPA.

    FIXED VERSION:
    - Always acquires both record_lock and audit_lock using acquire_locks, which
      enforces a global lock ordering.
    - Locks are held for the entire update, making the operation atomic.
    - We do not use non-blocking second-lock acquisition plus symmetric backoff
      anymore, so the atomic-violation livelock cannot occur.
    """
    while not stop_event.is_set():
        stats.inc("attempts", "registrar")

        with acquire_locks(record_lock, audit_lock):
            time.sleep(FIRST_LOCK_HOLD_SECONDS)
            student_record["gpa"] = round(student_record["gpa"] + 0.01, 2)
            student_record["last_update"] = "RegistrarService: GPA adjusted"
            stats.inc("successes", "registrar")
            time.sleep(0.002)

        time.sleep(0.001)


def advisor_service_update_contact(stop_event: threading.Event, stats: Stats):
    """
    AdvisorService updates address/contact info.

    FIXED VERSION:
    - Uses the same acquire_locks helper, so both services obey the same
      global lock order (no circular wait) even though this function passes
      locks in the opposite order.
    - Because lock acquisition is blocking and ordered, two threads
      cannot "dance" around each other in a livelock anymore.
    """
    while not stop_event.is_set():
        stats.inc("attempts", "advisor")

        with acquire_locks(audit_lock, record_lock):
            time.sleep(FIRST_LOCK_HOLD_SECONDS)
            student_record["address"] = "Seoul (verified)"
            student_record["last_update"] = "AdvisorService: Address verified"
            stats.inc("successes", "advisor")
            time.sleep(0.002)

        time.sleep(0.001)


def monitor(stop_event: threading.Event, stats: Stats):
    """
    Periodically prints progress and flags likely livelock:
    - High attempt growth (active spinning/retrying)
    - No commits for STALL_SECONDS

    In the fixed version, you should see continuous successes and the
    livelock warning should never trigger.
    """
    last = stats.snapshot()

    while not stop_event.is_set():
        time.sleep(MONITOR_INTERVAL_SECONDS)

        now = time.time()
        snap = stats.snapshot()

        attempts_now = snap["attempts"]["registrar"] + snap["attempts"]["advisor"]
        successes_now = snap["successes"]["registrar"] + snap["successes"]["advisor"]

        attempts_prev = last["attempts"]["registrar"] + last["attempts"]["advisor"]
        successes_prev = last["successes"]["registrar"] + last["successes"]["advisor"]

        d_attempts = attempts_now - attempts_prev
        d_successes = successes_now - successes_prev
        seconds_since_commit = now - snap["last_commit_time"]

        print("=== Monitor ===")
        print(f"Attempts : {snap['attempts']} (Δ {d_attempts} since last)")
        print(f"Successes: {snap['successes']} (Δ {d_successes} since last)")
        print(f"Failures : {snap['failures']}")
        print(f"Seconds since last commit: {seconds_since_commit:.3f}")

        if seconds_since_commit >= STALL_SECONDS and d_attempts >= MIN_ATTEMPTS_GROWTH:
            print("\n*** LIVELOCK DETECTED (likely) ***")
            print("Reason: High activity (many attempts) with no committed progress.")
            print("This should NOT happen in the fixed version.\n")

        last = snap


def main():
    stats = Stats()
    stop_event = threading.Event()

    t1 = threading.Thread(
        target=registrar_service_update_grade, name="RegistrarService", args=(stop_event, stats), daemon=True
    )
    t2 = threading.Thread(
        target=advisor_service_update_contact, name="AdvisorService", args=(stop_event, stats), daemon=True
    )
    tmon = threading.Thread(
        target=monitor, name="Monitor", args=(stop_event, stats), daemon=True
    )

    t1.start()
    t2.start()
    tmon.start()

    time.sleep(RUN_SECONDS)
    stop_event.set()

    t1.join(timeout=1.0)
    t2.join(timeout=1.0)
    tmon.join(timeout=1.0)

    snap = stats.snapshot()
    total_attempts = snap["attempts"]["registrar"] + snap["attempts"]["advisor"]
    total_successes = snap["successes"]["registrar"] + snap["successes"]["advisor"]
    total_failures = snap["failures"]["registrar"] + snap["failures"]["advisor"]

    print("\n=== Student Information Record Simulation (Final) ===")
    print(f"Run time (seconds): {RUN_SECONDS}")
    print("--- Totals ---")
    print(f"Attempts={total_attempts}, Successes={total_successes}, Failures={total_failures}")
    print("--- Final Student Record ---")
    print(student_record)

    if total_attempts > 200 and total_successes <= 2:
        print("\nResult: Likely LIVELOCK detected (high activity, little progress).")
    else:
        print("\nResult: Healthy progress observed; livelock resolved in fixed version.")


if __name__ == "__main__":
    main()

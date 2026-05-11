import threading
import time
import random
from dataclasses import dataclass, field

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


LIVEL0CK_MODE = False

BACKOFF_SECONDS = 0.005
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


def polite_backoff():
    """
    Backoff strategy.

    When LIVEL0CK_MODE is True we deliberately use deterministic backoff to
    *simulate* livelock. In normal operation it should be False so we use
    randomized jitter to break symmetry between threads.
    """
    if LIVEL0CK_MODE:

        time.sleep(BACKOFF_SECONDS)
    else:

        time.sleep(BACKOFF_SECONDS + random.uniform(0, BACKOFF_SECONDS))


def registrar_service_update_grade(stop_event: threading.Event, stats: Stats):
    """
    RegistrarService updates GPA.

    Lock order (FIXED canonical order): record_lock -> audit_lock
    Non-blocking acquisition of the first lock keeps the worker responsive,
    but both services now use *the same* lock order, which eliminates
    the circular-wait pattern that caused livelock.
    """
    while not stop_event.is_set():
        stats.inc("attempts", "registrar")


        if not record_lock.acquire(blocking=False):
            stats.inc("failures", "registrar")
            polite_backoff()
            continue

        try:
            time.sleep(FIRST_LOCK_HOLD_SECONDS)


            if not audit_lock.acquire(blocking=False):
                stats.inc("failures", "registrar")
                polite_backoff()
                continue

            try:
                student_record["gpa"] = round(student_record["gpa"] + 0.01, 2)
                student_record["last_update"] = "RegistrarService: GPA adjusted"
                stats.inc("successes", "registrar")
                time.sleep(0.002)
            finally:
                audit_lock.release()
        finally:
            record_lock.release()

        time.sleep(0.001)


def advisor_service_update_contact(stop_event: threading.Event, stats: Stats):
    """
    AdvisorService updates address/contact info.

    FIX: Use the *same* lock acquisition order as RegistrarService:
         record_lock -> audit_lock.

    With a consistent global lock ordering, we remove the possibility of
    a circular wait (and thus deadlock/livelock) even when using
    non-blocking acquisition plus backoff.
    """
    while not stop_event.is_set():
        stats.inc("attempts", "advisor")


        if not record_lock.acquire(blocking=False):
            stats.inc("failures", "advisor")
            polite_backoff()
            continue

        try:
            time.sleep(FIRST_LOCK_HOLD_SECONDS)


            if not audit_lock.acquire(blocking=False):
                stats.inc("failures", "advisor")
                polite_backoff()
                continue

            try:
                student_record["address"] = "Seoul (verified)"
                student_record["last_update"] = "AdvisorService: Address verified"
                stats.inc("successes", "advisor")
                time.sleep(0.002)
            finally:
                audit_lock.release()
        finally:
            record_lock.release()

        time.sleep(0.001)


def monitor(stop_event: threading.Event, stats: Stats):
    """
    Periodically prints progress and flags likely livelock:
    - High attempt growth (active spinning/retrying)
    - No commits for STALL_SECONDS

    With the fixes applied (common lock ordering + jittered backoff),
    the monitor should not report sustained livelock.
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
            print("Suggestion: Keep LIVEL0CK_MODE=False and ensure a single,")
            print("            consistent lock ordering across all services.\n")

        last = snap


def main():
    stats = Stats()
    stop_event = threading.Event()

    t1 = threading.Thread(
        target=registrar_service_update_grade,
        name="RegistrarService",
        args=(stop_event, stats),
        daemon=True,
    )
    t2 = threading.Thread(
        target=advisor_service_update_contact,
        name="AdvisorService",
        args=(stop_event, stats),
        daemon=True,
    )
    tmon = threading.Thread(
        target=monitor,
        name="Monitor",
        args=(stop_event, stats),
        daemon=True,
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
    print(f"LIVEL0CK_MODE: {LIVEL0CK_MODE}")
    print(f"Run time (seconds): {RUN_SECONDS}")
    print("--- Totals ---")
    print(f"Attempts={total_attempts}, Successes={total_successes}, Failures={total_failures}")
    print("--- Final Student Record ---")
    print(student_record)

    if total_attempts > 200 and total_successes <= 2:
        print("\nResult: Likely LIVELOCK detected (high activity, little progress).")
        print("Tip   : In the fixed version this should not trigger.")
        print("        If you re-enable the old livelock behaviour for teaching,")
        print("        set LIVEL0CK_MODE=True and restore opposite lock ordering.")
    else:
        print("\nResult: Progress observed; no livelock in this run.")


if __name__ == "__main__":
    main()

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



SOLUTION = "JITTER"

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


def polite_backoff_with_jitter():
    """
    SOLUTION 1: Add random jitter to break symmetry.
    This is the simplest fix - randomness ensures threads don't stay synchronized.
    """
    time.sleep(BACKOFF_SECONDS + random.uniform(0, BACKOFF_SECONDS))


def registrar_service_update_grade_jitter(stop_event: threading.Event, stats: Stats):
    """Registrar with jitter backoff to break livelock symmetry."""
    while not stop_event.is_set():
        stats.inc("attempts", "registrar")

        if not record_lock.acquire(blocking=False):
            stats.inc("failures", "registrar")
            polite_backoff_with_jitter()
            continue

        try:
            time.sleep(FIRST_LOCK_HOLD_SECONDS)

            if not audit_lock.acquire(blocking=False):
                stats.inc("failures", "registrar")
                polite_backoff_with_jitter()
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


def advisor_service_update_contact_jitter(stop_event: threading.Event, stats: Stats):
    """Advisor with jitter backoff to break livelock symmetry."""
    while not stop_event.is_set():
        stats.inc("attempts", "advisor")

        if not audit_lock.acquire(blocking=False):
            stats.inc("failures", "advisor")
            polite_backoff_with_jitter()
            continue

        try:
            time.sleep(FIRST_LOCK_HOLD_SECONDS)

            if not record_lock.acquire(blocking=False):
                stats.inc("failures", "advisor")
                polite_backoff_with_jitter()
                continue

            try:
                student_record["address"] = "Seoul (verified)"
                student_record["last_update"] = "AdvisorService: Address verified"
                stats.inc("successes", "advisor")
                time.sleep(0.002)
            finally:
                record_lock.release()
        finally:
            audit_lock.release()

        time.sleep(0.001)



unified_lock = threading.Lock()


def registrar_service_unified(stop_event: threading.Event, stats: Stats):
    """Registrar using single unified lock - no livelock possible."""
    while not stop_event.is_set():
        stats.inc("attempts", "registrar")
        
        with unified_lock:
            student_record["gpa"] = round(student_record["gpa"] + 0.01, 2)
            student_record["last_update"] = "RegistrarService: GPA adjusted"
            stats.inc("successes", "registrar")
            time.sleep(0.002)

        time.sleep(0.001)


def advisor_service_unified(stop_event: threading.Event, stats: Stats):
    """Advisor using single unified lock - no livelock possible."""
    while not stop_event.is_set():
        stats.inc("attempts", "advisor")
        
        with unified_lock:
            student_record["address"] = "Seoul (verified)"
            student_record["last_update"] = "AdvisorService: Address verified"
            stats.inc("successes", "advisor")
            time.sleep(0.002)

        time.sleep(0.001)



def registrar_service_ordered(stop_event: threading.Event, stats: Stats):
    """Registrar using consistent lock order: record_lock -> audit_lock."""
    while not stop_event.is_set():
        stats.inc("attempts", "registrar")

        with record_lock:
            with audit_lock:
                student_record["gpa"] = round(student_record["gpa"] + 0.01, 2)
                student_record["last_update"] = "RegistrarService: GPA adjusted"
                stats.inc("successes", "registrar")
                time.sleep(0.002)

        time.sleep(0.001)


def advisor_service_ordered(stop_event: threading.Event, stats: Stats):
    """Advisor using SAME lock order: record_lock -> audit_lock (not audit->record)."""
    while not stop_event.is_set():
        stats.inc("attempts", "advisor")


        with record_lock:
            with audit_lock:
                student_record["address"] = "Seoul (verified)"
                student_record["last_update"] = "AdvisorService: Address verified"
                stats.inc("successes", "advisor")
                time.sleep(0.002)

        time.sleep(0.001)


def monitor(stop_event: threading.Event, stats: Stats):
    """Monitor thread progress and detect potential livelock."""
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
            print("\n*** POTENTIAL LIVELOCK ***")
        else:
            print("✓ Progress is being made")

        last = snap


def main():
    stats = Stats()
    stop_event = threading.Event()

    print(f"\n=== Running with SOLUTION: {SOLUTION} ===\n")


    if SOLUTION == "JITTER":
        registrar_func = registrar_service_update_grade_jitter
        advisor_func = advisor_service_update_contact_jitter
    elif SOLUTION == "UNIFIED_LOCK":
        registrar_func = registrar_service_unified
        advisor_func = advisor_service_unified
    elif SOLUTION == "LOCK_ORDERING":
        registrar_func = registrar_service_ordered
        advisor_func = advisor_service_ordered
    else:
        raise ValueError(f"Unknown solution: {SOLUTION}")

    t1 = threading.Thread(
        target=registrar_func, name="RegistrarService", args=(stop_event, stats), daemon=True
    )
    t2 = threading.Thread(
        target=advisor_func, name="AdvisorService", args=(stop_event, stats), daemon=True
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

    print("\n=== Final Results ===")
    print(f"Solution: {SOLUTION}")
    print(f"Run time: {RUN_SECONDS} seconds")
    print(f"Attempts={total_attempts}, Successes={total_successes}, Failures={total_failures}")
    print(f"Success rate: {100*total_successes/total_attempts:.1f}%" if total_attempts > 0 else "N/A")
    print("\n--- Final Student Record ---")
    print(student_record)

    if total_attempts > 200 and total_successes <= 2:
        print("\n❌ LIVELOCK still present!")
    else:
        print(f"\n✓ LIVELOCK RESOLVED - {total_successes} successful operations completed")


if __name__ == "__main__":
    main()
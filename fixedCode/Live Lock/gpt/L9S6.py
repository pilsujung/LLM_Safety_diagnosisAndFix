import threading
import time
import random
from dataclasses import dataclass
from datetime import datetime


class ChatMessage:
    """Represents a chat message that can be updated concurrently."""
    def __init__(self, message_id: str, content: str):
        self.message_id = message_id
        self.content = content
        self.version = 0
        self.lock = threading.Lock()


        self.last_update_attempt_ts = None

    def __str__(self) -> str:
        return f"[Message {self.message_id}] v{self.version}: {self.content}"


@dataclass
class UpdateStats:
    attempts: int = 0
    successes: int = 0
    lock_misses: int = 0
    polite_backoffs: int = 0


class LivelockDetector:
    """
    Detects livelock heuristically:
    - No progress (version does not change) for NO_PROGRESS_SECONDS
    - And attempts keep happening (>= MIN_ATTEMPTS_WITHOUT_PROGRESS)
    """
    def __init__(
        self,
        no_progress_seconds: float = 0.60,
        min_attempts_without_progress: int = 20,
        check_interval_seconds: float = 0.05,
    ):
        self.NO_PROGRESS_SECONDS = no_progress_seconds
        self.MIN_ATTEMPTS_WITHOUT_PROGRESS = min_attempts_without_progress
        self.CHECK_INTERVAL_SECONDS = check_interval_seconds

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.livelock_detected = threading.Event()

        self._last_progress_ts = time.time()
        self._last_version = 0
        self._attempts_since_progress = 0

        self._watchdog_thread = None

    def start(self, initial_version: int = 0):
        with self._lock:
            self._last_progress_ts = time.time()
            self._last_version = initial_version
            self._attempts_since_progress = 0

        self._stop_event.clear()
        self.livelock_detected.clear()

        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="LivelockWatchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def stop(self):
        self._stop_event.set()
        t = self._watchdog_thread
        if t and t.is_alive():
            t.join(timeout=0.3)

    def note_attempt(self):
        with self._lock:
            self._attempts_since_progress += 1

    def note_progress(self, new_version: int):
        now = time.time()
        with self._lock:

            if new_version != self._last_version:
                self._last_version = new_version
                self._last_progress_ts = now
                self._attempts_since_progress = 0

    def _watchdog_loop(self):
        while not self._stop_event.is_set() and not self.livelock_detected.is_set():
            time.sleep(self.CHECK_INTERVAL_SECONDS)

            with self._lock:
                elapsed_no_progress = time.time() - self._last_progress_ts
                attempts = self._attempts_since_progress

            if (
                elapsed_no_progress >= self.NO_PROGRESS_SECONDS
                and attempts >= self.MIN_ATTEMPTS_WITHOUT_PROGRESS
            ):
                self.livelock_detected.set()





class ChatUpdateManager:
    """Coordinates concurrent updates and records logs/metrics."""
    def __init__(self, detector: LivelockDetector | None = None):
        self.update_log = []
        self.log_lock = threading.Lock()
        self.stats_by_actor: dict[str, UpdateStats] = {}
        self.detector = detector

    def _ts(self) -> str:
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def log_event(self, message: str):
        with self.log_lock:
            line = f"[{self._ts()}] {message}"
            self.update_log.append(line)
            print(line)

    def _stats(self, actor: str) -> UpdateStats:
        if actor not in self.stats_by_actor:
            self.stats_by_actor[actor] = UpdateStats()
        return self.stats_by_actor[actor]

    def polite_update_with_livelock(
        self,
        message: ChatMessage,
        new_content: str,
        actor: str,
        *,
        max_attempts: int = 50,
        polite_window_seconds: float = 0.10,
        symmetry_breaker: bool = False,
    ) -> bool:
        """
        'Polite' update approach — fixed to avoid livelock.

        Key change:
          - message.last_update_attempt_ts is treated as the time of the last
            *successful* update, not of every backoff/attempt.
          - We no longer refresh this timestamp in the backoff path.

        This guarantees:
          - If no one is making progress, the timestamp stops moving, so after
            polite_window_seconds someone will stop yielding and perform an update.
          - If at least one actor succeeds periodically, the livelock detector
            sees progress and will not trigger.
        """
        self.log_event(f"[INFO] {actor} wants to update content -> '{new_content}'")

        s = self._stats(actor)

        for attempt in range(1, max_attempts + 1):

            if self.detector and self.detector.livelock_detected.is_set():
                self.log_event(
                    f"[DETECTOR] Livelock detected. {actor} aborting on attempt {attempt}."
                )
                return False

            s.attempts += 1
            if self.detector:
                self.detector.note_attempt()

            acquired = message.lock.acquire(blocking=False)
            if not acquired:
                s.lock_misses += 1
                self.log_event(f"[RETRY] {actor} could not acquire lock (attempt {attempt}).")
                time.sleep(0.02 if not symmetry_breaker else random.uniform(0.01, 0.04))
                continue

            try:
                now = time.time()
                last = message.last_update_attempt_ts


                if last is not None and (now - last) < polite_window_seconds:
                    s.polite_backoffs += 1



                    self.log_event(
                        f"[BACKOFF] {actor} yielded (recent attempt detected) "
                        f"(attempt {attempt})."
                    )


                    time.sleep(0.03 if not symmetry_breaker else random.uniform(0.01, 0.06))
                    continue


                message.last_update_attempt_ts = now


                time.sleep(0.03 if not symmetry_breaker else random.uniform(0.01, 0.05))


                message.content = new_content
                message.version += 1
                s.successes += 1

                if self.detector:
                    self.detector.note_progress(message.version)

                self.log_event(f"[OK] {actor} updated successfully. New version={message.version}.")
                return True

            finally:
                message.lock.release()

        self.log_event(
            f"[FAIL] {actor} failed after {max_attempts} attempts "
            f"(attempts={s.attempts}, backoffs={s.polite_backoffs}, lock_misses={s.lock_misses})."
        )
        return False





def demonstrate_livelock(symmetry_breaker: bool = False):
    print("=" * 78)
    print("LIVELOCK SIMULATION: Chat Message Updates (with Livelock Detection)")
    print("=" * 78)
    print("Scenario: Two actors attempt to update the same message concurrently.")
    print("They use a 'polite' strategy. In the original version this could livelock;")
    print("in the fixed version, progress is guaranteed.")
    print()

    detector = LivelockDetector(
        no_progress_seconds=0.60,
        min_attempts_without_progress=20,
        check_interval_seconds=0.05,
    )
    manager = ChatUpdateManager(detector=detector)

    message = ChatMessage(message_id="A-001", content="Original message")

    detector.start(initial_version=message.version)

    def actor1():
        manager.polite_update_with_livelock(
            message,
            "Actor 1 updated message",
            "Actor 1",
            max_attempts=50,
            polite_window_seconds=0.10,
            symmetry_breaker=symmetry_breaker,
        )

    def actor2():
        manager.polite_update_with_livelock(
            message,
            "Actor 2 updated message",
            "Actor 2",
            max_attempts=50,
            polite_window_seconds=0.10,
            symmetry_breaker=symmetry_breaker,
        )

    t1 = threading.Thread(target=actor1, name="Actor1Thread")
    t2 = threading.Thread(target=actor2, name="Actor2Thread")

    start = time.time()
    t1.start()
    time.sleep(0.005)
    t2.start()

    t1.join()
    t2.join()

    detector.stop()
    elapsed = time.time() - start

    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(f"Elapsed time: {elapsed:.2f}s")
    print(f"Livelock detected: {detector.livelock_detected.is_set()}")
    print(f"Final message state: {message}")
    print()

    for actor, st in manager.stats_by_actor.items():
        print(
            f"{actor}: attempts={st.attempts}, successes={st.successes}, "
            f"backoffs={st.polite_backoffs}, lock_misses={st.lock_misses}"
        )


def demonstrate_deadlock_comparison():
    print("\n" + "=" * 78)
    print("BONUS: Deadlock vs Livelock (Safe Demonstration)")
    print("=" * 78)

    lock1 = threading.Lock()
    lock2 = threading.Lock()

    def deadlock_thread1():
        with lock1:
            print("[DEADLOCK] Thread 1 acquired lock1, waiting for lock2...")
            time.sleep(0.1)
            with lock2:
                print("[DEADLOCK] Thread 1 acquired both locks.")

    def deadlock_thread2():
        with lock2:
            print("[DEADLOCK] Thread 2 acquired lock2, waiting for lock1...")
            time.sleep(0.1)
            with lock1:
                print("[DEADLOCK] Thread 2 acquired both locks.")

    t1 = threading.Thread(target=deadlock_thread1, daemon=True)
    t2 = threading.Thread(target=deadlock_thread2, daemon=True)

    t1.start()
    t2.start()

    time.sleep(0.5)
    print("\n[INFO] If this were a true deadlock in non-daemon threads, the program might hang.")
    print("[INFO] Deadlock: blocked threads, little/no CPU usage.")
    print("[INFO] Livelock: active threads, CPU used, but no progress.\n")


if __name__ == "__main__":
    demonstrate_livelock(symmetry_breaker=False)




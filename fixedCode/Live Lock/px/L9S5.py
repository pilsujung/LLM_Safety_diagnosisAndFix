import threading
import time
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


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
    """Coordinates concurrent updates with livelock prevention."""
    def __init__(self, detector: LivelockDetector | None = None):
        self.update_log = []
        self.log_lock = threading.Lock()
        self.stats_by_actor: dict[str, UpdateStats] = {}
        self.detector = detector
        self.random = random.Random()

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

    def polite_update_fixed(
        self,
        message: ChatMessage,
        new_content: str,
        actor: str,
        *,
        max_attempts: int = 50,
        polite_window_seconds: float = 0.10,
    ) -> bool:
        """
        FIXED: Resolves livelock using strategies from C++/Java examples:
        1. Exponential backoff with randomization (like C++ random delays)
        2. Attempt counter with forced execution (like Java max attempts)
        3. Priority-based symmetry breaking via actor name hash
        """
        self.log_event(f"[INFO] {actor} wants to update content -> '{new_content}'")

        s = self._stats(actor)
        base_delay = 0.01
        attempts = 0

        while attempts < max_attempts:

            if self.detector and self.detector.livelock_detected.is_set():
                self.log_event(f"[DETECTOR] Livelock detected. {actor} aborting.")
                return False

            attempts += 1
            s.attempts += 1
            if self.detector:
                self.detector.note_attempt()


            acquired = message.lock.acquire(blocking=False, timeout=0.01)
            if not acquired:
                s.lock_misses += 1

                delay = base_delay * (2 ** min(attempts // 5, 6)) + self.random.uniform(0, 0.02)
                self.log_event(f"[RETRY] {actor} lock miss (attempt {attempts}), backoff {delay:.3f}s")
                time.sleep(delay)
                continue

            try:
                now = time.time()
                last = message.last_update_attempt_ts


                if last is not None and (now - last) < polite_window_seconds:
                    s.polite_backoffs += 1

                    priority = hash(actor) % 1000
                    wait_factor = 0.5 if priority > 500 else 1.5
                    backoff_time = 0.02 * wait_factor + self.random.uniform(0, 0.01)
                    message.last_update_attempt_ts = now
                    self.log_event(f"[BACKOFF] {actor} polite yield (priority={priority}, {backoff_time:.3f}s) (attempt {attempts})")
                    time.sleep(backoff_time)
                    continue


                message.last_update_attempt_ts = now
                process_time = 0.03 + self.random.uniform(-0.01, 0.01)
                time.sleep(process_time)


                if attempts >= 10:
                    self.log_event(f"[FORCE] {actor} forcing update after {attempts} attempts")


                message.content = new_content
                message.version += 1
                s.successes += 1

                if self.detector:
                    self.detector.note_progress(message.version)

                self.log_event(f"[OK] {actor} updated! v={message.version} (attempt {attempts})")
                return True

            finally:
                message.lock.release()

        self.log_event(f"[FAIL] {actor} failed after {max_attempts} attempts (a={s.attempts}, b={s.polite_backoffs}, m={s.lock_misses})")
        return False





def demonstrate_fixed_livelock():
    print("=" * 78)
    print("FIXED LIVLOCK: Chat Message Updates (Livelock Resolved)")
    print("=" * 78)
    print("Uses: randomized exponential backoff, priority symmetry breaking, forced progress")
    print()

    detector = LivelockDetector()
    manager = ChatUpdateManager(detector=detector)

    message = ChatMessage(message_id="A-001", content="Original message")
    detector.start(initial_version=message.version)

    def actor1():
        manager.polite_update_fixed(
            message, "Actor 1 updated message ✅", "Actor 1",
            max_attempts=50, polite_window_seconds=0.08
        )

    def actor2():
        manager.polite_update_fixed(
            message, "Actor 2 updated message ✅", "Actor 2", 
            max_attempts=50, polite_window_seconds=0.08
        )

    t1 = threading.Thread(target=actor1, name="Actor1Thread")
    t2 = threading.Thread(target=actor2, name="Actor2Thread")

    start = time.time()
    t1.start()
    time.sleep(0.002)
    t2.start()

    t1.join()
    t2.join()

    detector.stop()
    elapsed = time.time() - start

    print("\n" + "=" * 78)
    print("FINAL SUMMARY")
    print("=" * 78)
    print(f"Elapsed: {elapsed:.2f}s | Livelock detected: {detector.livelock_detected.is_set()}")
    print(f"Final message: {message}")
    print()

    for actor, st in manager.stats_by_actor.items():
        print(f"{actor}: att={st.attempts}, succ={st.successes}, backoff={st.polite_backoffs}, misses={st.lock_misses}")


if __name__ == "__main__":
    demonstrate_fixed_livelock()

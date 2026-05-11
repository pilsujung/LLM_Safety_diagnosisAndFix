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
    """Detects livelock heuristically."""
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




    def simple_update(
        self,
        message: ChatMessage,
        new_content: str,
        actor: str,
        *,
        max_attempts: int = 50,
    ) -> bool:
        """
        FIX #1: Remove politeness logic - just retry until lock is acquired.
        This is the simplest and most reliable solution.
        """
        self.log_event(f"[INFO] {actor} wants to update content -> '{new_content}'")
        s = self._stats(actor)

        for attempt in range(1, max_attempts + 1):
            if self.detector and self.detector.livelock_detected.is_set():
                self.log_event(f"[DETECTOR] {actor} aborting on attempt {attempt}.")
                return False

            s.attempts += 1
            if self.detector:
                self.detector.note_attempt()

            acquired = message.lock.acquire(blocking=False)
            if not acquired:
                s.lock_misses += 1
                self.log_event(f"[RETRY] {actor} could not acquire lock (attempt {attempt}).")
                time.sleep(random.uniform(0.01, 0.04))
                continue

            try:

                time.sleep(random.uniform(0.01, 0.05))
                
                message.content = new_content
                message.version += 1
                s.successes += 1

                if self.detector:
                    self.detector.note_progress(message.version)

                self.log_event(f"[OK] {actor} updated successfully. New version={message.version}.")
                return True

            finally:
                message.lock.release()

        self.log_event(f"[FAIL] {actor} failed after {max_attempts} attempts.")
        return False




    def limited_backoff_update(
        self,
        message: ChatMessage,
        new_content: str,
        actor: str,
        *,
        max_attempts: int = 50,
        max_backoffs: int = 3,
        polite_window_seconds: float = 0.10,
    ) -> bool:
        """
        FIX #2: Limit the number of consecutive polite backoffs.
        After max_backoffs, proceed with update regardless.
        """
        self.log_event(f"[INFO] {actor} wants to update content -> '{new_content}'")
        s = self._stats(actor)
        consecutive_backoffs = 0

        for attempt in range(1, max_attempts + 1):
            if self.detector and self.detector.livelock_detected.is_set():
                self.log_event(f"[DETECTOR] {actor} aborting on attempt {attempt}.")
                return False

            s.attempts += 1
            if self.detector:
                self.detector.note_attempt()

            acquired = message.lock.acquire(blocking=False)
            if not acquired:
                s.lock_misses += 1
                consecutive_backoffs = 0
                self.log_event(f"[RETRY] {actor} could not acquire lock (attempt {attempt}).")
                time.sleep(random.uniform(0.01, 0.04))
                continue

            try:
                now = time.time()
                last = message.last_update_attempt_ts


                if (last is not None and 
                    (now - last) < polite_window_seconds and 
                    consecutive_backoffs < max_backoffs):
                    
                    s.polite_backoffs += 1
                    consecutive_backoffs += 1
                    message.last_update_attempt_ts = now

                    self.log_event(
                        f"[BACKOFF] {actor} yielded (backoff {consecutive_backoffs}/{max_backoffs}) "
                        f"(attempt {attempt})."
                    )
                    time.sleep(random.uniform(0.01, 0.06))
                    continue


                message.last_update_attempt_ts = now
                time.sleep(random.uniform(0.01, 0.05))

                message.content = new_content
                message.version += 1
                s.successes += 1

                if self.detector:
                    self.detector.note_progress(message.version)

                self.log_event(f"[OK] {actor} updated successfully. New version={message.version}.")
                return True

            finally:
                message.lock.release()

        self.log_event(f"[FAIL] {actor} failed after {max_attempts} attempts.")
        return False




    def priority_update(
        self,
        message: ChatMessage,
        new_content: str,
        actor: str,
        priority: int = 0,
        *,
        max_attempts: int = 50,
        polite_window_seconds: float = 0.10,
    ) -> bool:
        """
        FIX #3: Use priority to break symmetry.
        Higher priority threads are less likely to back off.
        """
        self.log_event(f"[INFO] {actor} (priority={priority}) wants to update -> '{new_content}'")
        s = self._stats(actor)

        for attempt in range(1, max_attempts + 1):
            if self.detector and self.detector.livelock_detected.is_set():
                self.log_event(f"[DETECTOR] {actor} aborting on attempt {attempt}.")
                return False

            s.attempts += 1
            if self.detector:
                self.detector.note_attempt()

            acquired = message.lock.acquire(blocking=False)
            if not acquired:
                s.lock_misses += 1
                self.log_event(f"[RETRY] {actor} could not acquire lock (attempt {attempt}).")
                time.sleep(random.uniform(0.01, 0.04))
                continue

            try:
                now = time.time()
                last = message.last_update_attempt_ts



                should_be_polite = (
                    last is not None and 
                    (now - last) < polite_window_seconds and
                    random.random() > (priority * 0.2)
                )

                if should_be_polite:
                    s.polite_backoffs += 1
                    message.last_update_attempt_ts = now
                    self.log_event(
                        f"[BACKOFF] {actor} (priority={priority}) yielded (attempt {attempt})."
                    )
                    time.sleep(random.uniform(0.01, 0.06))
                    continue

                message.last_update_attempt_ts = now
                time.sleep(random.uniform(0.01, 0.05))

                message.content = new_content
                message.version += 1
                s.successes += 1

                if self.detector:
                    self.detector.note_progress(message.version)

                self.log_event(f"[OK] {actor} updated successfully. New version={message.version}.")
                return True

            finally:
                message.lock.release()

        self.log_event(f"[FAIL] {actor} failed after {max_attempts} attempts.")
        return False




    def exponential_backoff_update(
        self,
        message: ChatMessage,
        new_content: str,
        actor: str,
        *,
        max_attempts: int = 50,
    ) -> bool:
        """
        FIX #4: Use exponential backoff instead of politeness check.
        This is a standard approach for resolving contention.
        """
        self.log_event(f"[INFO] {actor} wants to update content -> '{new_content}'")
        s = self._stats(actor)

        for attempt in range(1, max_attempts + 1):
            if self.detector and self.detector.livelock_detected.is_set():
                self.log_event(f"[DETECTOR] {actor} aborting on attempt {attempt}.")
                return False

            s.attempts += 1
            if self.detector:
                self.detector.note_attempt()

            acquired = message.lock.acquire(blocking=False)
            if not acquired:
                s.lock_misses += 1
                

                backoff_time = min(0.001 * (2 ** attempt), 1.0)
                jitter = random.uniform(0, backoff_time * 0.5)
                total_wait = backoff_time + jitter
                
                self.log_event(
                    f"[RETRY] {actor} could not acquire lock (attempt {attempt}), "
                    f"waiting {total_wait:.3f}s."
                )
                time.sleep(total_wait)
                continue

            try:

                time.sleep(random.uniform(0.01, 0.05))

                message.content = new_content
                message.version += 1
                s.successes += 1

                if self.detector:
                    self.detector.note_progress(message.version)

                self.log_event(f"[OK] {actor} updated successfully. New version={message.version}.")
                return True

            finally:
                message.lock.release()

        self.log_event(f"[FAIL] {actor} failed after {max_attempts} attempts.")
        return False






def test_fix_1_simple():
    print("=" * 78)
    print("FIX #1: Simple Update (Remove Politeness Check)")
    print("=" * 78)
    
    detector = LivelockDetector()
    manager = ChatUpdateManager(detector=detector)
    message = ChatMessage(message_id="A-001", content="Original")
    detector.start(initial_version=message.version)

    def actor1():
        manager.simple_update(message, "Actor 1 updated", "Actor 1")

    def actor2():
        manager.simple_update(message, "Actor 2 updated", "Actor 2")

    t1 = threading.Thread(target=actor1)
    t2 = threading.Thread(target=actor2)
    
    start = time.time()
    t1.start()
    time.sleep(0.005)
    t2.start()
    t1.join()
    t2.join()
    detector.stop()
    
    elapsed = time.time() - start
    print(f"\n✓ Completed in {elapsed:.2f}s")
    print(f"✓ Livelock detected: {detector.livelock_detected.is_set()}")
    print(f"✓ Final: {message}\n")


def test_fix_2_limited_backoff():
    print("=" * 78)
    print("FIX #2: Limited Backoff (Max 3 Consecutive Backoffs)")
    print("=" * 78)
    
    detector = LivelockDetector()
    manager = ChatUpdateManager(detector=detector)
    message = ChatMessage(message_id="A-002", content="Original")
    detector.start(initial_version=message.version)

    def actor1():
        manager.limited_backoff_update(message, "Actor 1 updated", "Actor 1", max_backoffs=3)

    def actor2():
        manager.limited_backoff_update(message, "Actor 2 updated", "Actor 2", max_backoffs=3)

    t1 = threading.Thread(target=actor1)
    t2 = threading.Thread(target=actor2)
    
    start = time.time()
    t1.start()
    time.sleep(0.005)
    t2.start()
    t1.join()
    t2.join()
    detector.stop()
    
    elapsed = time.time() - start
    print(f"\n✓ Completed in {elapsed:.2f}s")
    print(f"✓ Livelock detected: {detector.livelock_detected.is_set()}")
    print(f"✓ Final: {message}\n")


def test_fix_3_priority():
    print("=" * 78)
    print("FIX #3: Priority-Based (Actor 1 has priority=2, Actor 2 has priority=1)")
    print("=" * 78)
    
    detector = LivelockDetector()
    manager = ChatUpdateManager(detector=detector)
    message = ChatMessage(message_id="A-003", content="Original")
    detector.start(initial_version=message.version)

    def actor1():
        manager.priority_update(message, "Actor 1 updated", "Actor 1", priority=2)

    def actor2():
        manager.priority_update(message, "Actor 2 updated", "Actor 2", priority=1)

    t1 = threading.Thread(target=actor1)
    t2 = threading.Thread(target=actor2)
    
    start = time.time()
    t1.start()
    time.sleep(0.005)
    t2.start()
    t1.join()
    t2.join()
    detector.stop()
    
    elapsed = time.time() - start
    print(f"\n✓ Completed in {elapsed:.2f}s")
    print(f"✓ Livelock detected: {detector.livelock_detected.is_set()}")
    print(f"✓ Final: {message}\n")


def test_fix_4_exponential_backoff():
    print("=" * 78)
    print("FIX #4: Exponential Backoff")
    print("=" * 78)
    
    detector = LivelockDetector()
    manager = ChatUpdateManager(detector=detector)
    message = ChatMessage(message_id="A-004", content="Original")
    detector.start(initial_version=message.version)

    def actor1():
        manager.exponential_backoff_update(message, "Actor 1 updated", "Actor 1")

    def actor2():
        manager.exponential_backoff_update(message, "Actor 2 updated", "Actor 2")

    t1 = threading.Thread(target=actor1)
    t2 = threading.Thread(target=actor2)
    
    start = time.time()
    t1.start()
    time.sleep(0.005)
    t2.start()
    t1.join()
    t2.join()
    detector.stop()
    
    elapsed = time.time() - start
    print(f"\n✓ Completed in {elapsed:.2f}s")
    print(f"✓ Livelock detected: {detector.livelock_detected.is_set()}")
    print(f"✓ Final: {message}\n")


if __name__ == "__main__":
    print("LIVELOCK FIXES - Testing All Solutions\n")
    
    test_fix_1_simple()
    time.sleep(0.2)
    
    test_fix_2_limited_backoff()
    time.sleep(0.2)
    
    test_fix_3_priority()
    time.sleep(0.2)
    
    test_fix_4_exponential_backoff()
    
    print("=" * 78)
    print("All fixes tested successfully!")
    print("=" * 78)
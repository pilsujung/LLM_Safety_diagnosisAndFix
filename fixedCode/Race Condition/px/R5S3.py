import threading
import time
import random
from dataclasses import dataclass
from collections import Counter, defaultdict

@dataclass
class Seat:
    seat_id: int
    booked_by: int | None = None

class TicketOffice:
    def __init__(self, n_seats: int, use_lock: bool):
        self.seats = [Seat(i) for i in range(n_seats)]
        self.bookings: list[tuple[int, int, float]] = []
        self.lock = threading.Lock() if use_lock else None

    def book_one(self, customer_id: int, critical_delay: float) -> bool:
        if self.lock:
            with self.lock:
                return self._book_impl(customer_id, critical_delay)
        return self._book_impl(customer_id, critical_delay)

    def _book_impl(self, customer_id: int, critical_delay: float) -> bool:
        for seat in self.seats:
            if seat.booked_by is None:
                if critical_delay:
                    time.sleep(critical_delay * random.random())
                seat.booked_by = customer_id
                self.bookings.append((seat.seat_id, customer_id, time.time()))
                return True
        return False

class RaceDetector(threading.Thread):
    def __init__(self, office: TicketOffice, stop_event: threading.Event, interval: float = 0.001):
        super().__init__(daemon=True)
        self.office = office
        self.stop_event = stop_event
        self.interval = interval
        self.detected = False
        self._reported_seats: set[int] = set()

    def run(self):
        total_seats = len(self.office.seats)
        while not self.stop_event.is_set():
            snap = self.office.bookings[:]
            if len(snap) > total_seats and not self.detected:
                self.detected = True
                print(f"[RACE DETECTED] bookings({len(snap)}) exceeded seats({total_seats}) => possible oversell.")

            counts = Counter(seat_id for seat_id, _, _ in snap)
            dups = [sid for sid, c in counts.items() if c > 1]
            for sid in dups:
                if sid in self._reported_seats:
                    continue
                self._reported_seats.add(sid)
                buyers = [cid for s, cid, _ in snap if s == sid]
                self.detected = True
                print(f"[RACE DETECTED] seat {sid} sold multiple times: buyers={buyers}")
            time.sleep(self.interval)

def final_report(office: TicketOffice):
    seats = office.seats
    bookings = office.bookings[:]
    total_seats = len(seats)

    counts = Counter(seat_id for seat_id, _, _ in bookings)
    dup_seats = sorted([sid for sid, c in counts.items() if c > 1])
    oversold = max(0, len(bookings) - total_seats)

    booked_in_state = {s.seat_id for s in seats if s.booked_by is not None}
    booked_in_log = {sid for sid, _, _ in bookings}
    state_only = sorted(booked_in_state - booked_in_log)
    log_only = sorted(booked_in_log - booked_in_state)

    dup_details = defaultdict(list)
    for sid, cid, _ in bookings:
        if counts[sid] > 1:
            dup_details[sid].append(cid)

    print("\n========== FINAL REPORT ==========")
    print(f"- Total seats: {total_seats}")
    print(f"- Successful booking log entries: {len(bookings)}")
    print(f"- Oversold (log-based): {oversold}")
    print(f"- Duplicate-sold seats: {dup_seats if dup_seats else 'None'}")
    if dup_seats:
        for sid in dup_seats[:10]:
            print(f" * Seat {sid}: buyer records={dup_details[sid]}")
        if len(dup_seats) > 10:
            print(f" ... (showing 10 of {len(dup_seats)} duplicate seats)")

    print(f"- Booked in seat state only (missing in log): {state_only if state_only else 'None'}")
    print(f"- Present in log only (seat state still empty): {log_only if log_only else 'None'}")
    print("=================================\n")

def run_simulation(
    n_seats: int = 5,
    n_customers: int = 200,
    attempts_per_customer: int = 5,
    critical_delay: float = 0.0008,
    use_lock: bool = False
):
    office = TicketOffice(n_seats=n_seats, use_lock=use_lock)
    stop_event = threading.Event()
    detector = RaceDetector(office, stop_event)
    detector.start()

    start_barrier = threading.Barrier(n_customers + 1)

    def customer_worker(customer_id: int):
        start_barrier.wait()
        for _ in range(attempts_per_customer):
            if office.book_one(customer_id, critical_delay):
                return
        time.sleep(0)

    threads = [threading.Thread(target=customer_worker, args=(cid,), name=f"C{cid}") for cid in range(n_customers)]
    for t in threads:
        t.start()

    start_barrier.wait()
    for t in threads:
        t.join()

    stop_event.set()
    detector.join(timeout=0.1)

    mode = "SAFE (locking enabled)" if use_lock else "UNSAFE (no lock)"
    print(f"\n[SIMULATION COMPLETE] mode={mode}, detector.detected={detector.detected}")
    final_report(office)

if __name__ == "__main__":
    print("Running UNSAFE mode:")
    run_simulation(use_lock=False)
    print("\nRunning SAFE mode:")
    run_simulation(use_lock=True)

import threading
import time
import random
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
from queue import Queue

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(threadName)s - %(message)s")


class SeatStatus(Enum):
    AVAILABLE = "available"
    BOOKED = "booked"


class BookingError(Exception):
    pass


@dataclass
class Seat:
    seat_id: int
    status: SeatStatus = SeatStatus.AVAILABLE
    customer_id: int | None = None
    booking_time: datetime | None = None
    price: float = field(default_factory=lambda: random.uniform(50, 200))


@dataclass
class Customer:
    customer_id: int
    name: str
    budget: float
    booked_seats: list[int] = field(default_factory=list)
    attempts: int = 0
    successes: int = 0


class TicketBookingSystem:
    def __init__(self, total_seats: int):
        self.seats = {i: Seat(i) for i in range(1, total_seats + 1)}
        self.revenue = 0.0
        self.failed = 0
        self.history: list[dict] = []
        self.stats = defaultdict(lambda: {"attempts": 0, "successes": 0})
        self.q: Queue[tuple[int, str]] = Queue()


        self.booking_lock = threading.Lock()
        self.revenue_lock = threading.Lock()
        self.history_lock = threading.Lock()
        self.stats_lock = threading.Lock()


        self._audit_lock = threading.Lock()
        self._race_counters = defaultdict(int)
        self._race_events: list[dict] = []


    def available_seats(self) -> list[int]:

        with self.booking_lock:
            return [sid for sid, s in self.seats.items() if s.status == SeatStatus.AVAILABLE]

    def total_cost(self, seat_ids: list[int]) -> float:

        with self.booking_lock:
            return sum(self.seats[sid].price for sid in seat_ids)

    def all_available(self, seat_ids: list[int]) -> bool:

        return all(self.seats[sid].status == SeatStatus.AVAILABLE for sid in seat_ids)

    def _notify(self, customer_id: int, msg: str):
        self.q.put((customer_id, msg))

    def _log(self, customer_id: int, seat_ids: list[int], success: bool):

        with self.history_lock:
            self.history.append(
                {"ts": datetime.now(), "customer_id": customer_id, "seat_ids": seat_ids, "success": success}
            )
            if not success:
                self.failed += 1

    def _audit(self, typ: str, **details):
        with self._audit_lock:
            self._race_counters[typ] += 1
            if len(self._race_events) < 200:
                self._race_events.append(
                    {"ts": datetime.now(), "thread": threading.current_thread().name, "type": typ, **details}
                )


    def process_payment(self, customer: Customer, cost: float) -> bool:
        time.sleep(random.uniform(0.05, 0.15))
        if customer.budget >= cost:
            customer.budget -= cost

            with self.revenue_lock:
                self.revenue += cost
            return True
        return False

    def book_seats(self, seat_ids: list[int], customer: Customer) -> bool:
        logging.info(f"{customer.name} attempting {seat_ids}")
        customer.attempts += 1
        

        with self.stats_lock:
            self.stats[customer.customer_id]["attempts"] += 1



        with self.booking_lock:
            try:

                if not self.all_available(seat_ids):
                    raise BookingError("Some seats are not available")

                cost = self.total_cost(seat_ids)




                for sid in seat_ids:
                    self.seats[sid].status = SeatStatus.BOOKED
                    self.seats[sid].customer_id = customer.customer_id
                    self.seats[sid].booking_time = datetime.now()



                booking_lock_released = True
            except BookingError as e:

                self._notify(customer.customer_id, f"Booking failed: {e}")
                self._log(customer.customer_id, seat_ids, False)
                logging.warning(f"{customer.name} failed {seat_ids}: {e}")
                return False


        payment_success = self.process_payment(customer, cost)


        with self.booking_lock:
            if payment_success:

                for sid in seat_ids:
                    customer.booked_seats.append(sid)

                customer.successes += 1
                with self.stats_lock:
                    self.stats[customer.customer_id]["successes"] += 1

                self._notify(customer.customer_id, f"Booked {seat_ids} for ${cost:.2f}")
                self._log(customer.customer_id, seat_ids, True)
                logging.info(f"{customer.name} success {seat_ids} (${cost:.2f})")
                return True
            else:

                for sid in seat_ids:
                    self.seats[sid].status = SeatStatus.AVAILABLE
                    self.seats[sid].customer_id = None
                    self.seats[sid].booking_time = None


                with self.revenue_lock:
                    self.revenue -= cost

                raise BookingError("Insufficient funds")


        self._notify(customer.customer_id, "Booking failed: Unknown error")
        self._log(customer.customer_id, seat_ids, False)
        logging.warning(f"{customer.name} failed {seat_ids}: Unknown error")
        return False


    def seat_status_counts(self):
        with self.booking_lock:
            c = defaultdict(int)
            for s in self.seats.values():
                c[s.status] += 1
            return dict(c)

    def race_audit_report(self, customers: list[Customer]):
        with self.booking_lock:
            final_booked = [s for s in self.seats.values() if s.status == SeatStatus.BOOKED]
            max_possible_revenue = sum(s.price for s in final_booked)

        with self.revenue_lock:
            revenue = self.revenue
        
        revenue_exceeds = (revenue - max_possible_revenue) > 1e-6

        with self.history_lock:
            history = list(self.history)

        seat_success_counts = defaultdict(int)
        for h in history:
            if h["success"]:
                for sid in h["seat_ids"]:
                    seat_success_counts[sid] += 1
        duplicated_success = {sid: n for sid, n in seat_success_counts.items() if n > 1}

        mismatches = []
        with self.booking_lock:
            for cst in customers:
                for sid in cst.booked_seats:
                    s = self.seats.get(sid)
                    if s and s.customer_id != cst.customer_id:
                        mismatches.append((sid, cst.customer_id, s.customer_id, s.status.value))

        with self._audit_lock:
            counters = dict(self._race_counters)
            events = list(self._race_events)

        print("\n=== Race Condition Audit ===")
        print("[In-flight signals]")
        for k in ("TOCTOU_STATE_CHANGED_BEFORE_COMMIT", "OVERWRITE_NON_AVAILABLE_SEAT", "POST_COMMIT_MISMATCH"):
            print(f"  {k}: {counters.get(k, 0)}")

        print("\n[Post-run checks]")
        print(f"  Revenue: ${revenue:.2f}")
        print(f"  Max possible revenue (final unique booked seats): ${max_possible_revenue:.2f}")
        print(f"  Revenue exceeds max possible? {revenue_exceeds}")
        print(f"  Seats in multiple SUCCESS attempts: {len(duplicated_success)}")
        print(f"  Customer-claim vs final-owner mismatches: {len(mismatches)}")

        if events:
            print("\n[Sample events] (up to 8)")
            for e in events[:8]:
                meta = {k: v for k, v in e.items() if k not in ("ts", "thread")}
                print(f"  - {e['ts']} | {e['thread']} | {meta}")

        likely = (
            any(counters.get(k, 0) > 0 for k in ("TOCTOU_STATE_CHANGED_BEFORE_COMMIT",
                                                "OVERWRITE_NON_AVAILABLE_SEAT",
                                                "POST_COMMIT_MISMATCH"))
            or revenue_exceeds
            or bool(duplicated_success)
            or bool(mismatches)
        )
        print(f"\nVerdict: race likely occurred? {likely}")


def customer_worker(sys: TicketBookingSystem, customer: Customer, n_seats: int):
    while True:
        avail = sys.available_seats()
        if len(avail) < n_seats:
            break
        seat_ids = random.sample(avail, n_seats)
        sys.book_seats(seat_ids, customer)
        time.sleep(random.uniform(0.2, 0.7))


def notification_worker(sys: TicketBookingSystem):
    while True:
        cid, msg = sys.q.get()
        logging.info(f"Notify {cid}: {msg}")
        time.sleep(0.05)


def report(sys: TicketBookingSystem, customers: list[Customer]):
    print("\n=== Booking System Report ===")
    
    with sys.revenue_lock:
        revenue = sys.revenue
    
    with sys.history_lock:
        failed = sys.failed
    
    print(f"Total Revenue: ${revenue:.2f}")
    print(f"Failed Bookings: {failed}")

    print("\nSeat Status:")
    for st, cnt in sys.seat_status_counts().items():
        print(f"  {st.value}: {cnt}")

    print("\nCustomer Stats:")
    with sys.stats_lock:
        stats = dict(sys.stats)
    
    for cid, st in stats.items():
        rate = (st["successes"] / st["attempts"] * 100) if st["attempts"] else 0.0
        print(f"  Customer {cid}: attempts={st['attempts']}, successes={st['successes']}, success_rate={rate:.1f}%")

    sys.race_audit_report(customers)


def main():
    sys = TicketBookingSystem(total_seats=20)
    customers = [Customer(i, f"Customer_{i}", random.uniform(300, 1000)) for i in range(10)]

    threading.Thread(target=notification_worker, args=(sys,), daemon=True).start()

    threads = []
    for c in customers:
        t = threading.Thread(target=customer_worker, args=(sys, c, random.randint(2, 4)))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    report(sys, customers)


if __name__ == "__main__":
    main()
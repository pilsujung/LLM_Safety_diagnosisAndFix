import threading
import time
import random
from tabulate import tabulate
import sys
from datetime import datetime

class Theater:
    def __init__(self, rows=5, seats_per_row=10):
        self.seating = {}
        for row in range(1, rows + 1):
            for seat in range(1, seats_per_row + 1):
                seat_id = f"{chr(64+row)}{seat}"
                self.seating[seat_id] = True
        self.bookings = {}
        self.booking_attempts = {}
        self.success_claims = {}
                                               
        self._locks = {seat_id: threading.Lock() for seat_id in self.seating}

    def is_seat_available(self, seat_id):
        return self.seating.get(seat_id, False)

                                             
    def reserve_seat_safe(self, seat_id, user_id):
        if seat_id not in self._locks:
            return False
        lock = self._locks[seat_id]
        
                                                           
        if seat_id not in self.booking_attempts:
            self.booking_attempts[seat_id] = []
        self.booking_attempts[seat_id].append(user_id)

        with lock:
            if self.is_seat_available(seat_id):
                time.sleep(random.uniform(0.01, 0.05))                     
                self.seating[seat_id] = False
                self.bookings[seat_id] = user_id
                if seat_id not in self.success_claims:
                    self.success_claims[seat_id] = []
                self.success_claims[seat_id].append((user_id, time.time()))
                return True
            return False

                                                                                            

def simulate_user(theater, user_id, target_seats):
                                                   
    for seat_id in target_seats:
        success = theater.reserve_seat_safe(seat_id, user_id)                    
        if success:
            print(f"User-{user_id} successfully booked seat {seat_id}")
            break
        else:
            print(f"User-{user_id} failed to book seat {seat_id}, trying another...")
    else:
        print(f"User-{user_id} couldn't book any of their preferred seats")

                                                      
def main():
    print("=== FIXED: ATOMIC VIOLATION RESOLVED WITH PER-SEAT LOCKS ===")
    theater = Theater(rows=5, seats_per_row=10)
    users = [
        (1, ["A1", "A2", "A3"]), (2, ["A1", "B1", "C1"]), (3, ["A1", "A2", "B2"]),
        (4, ["B1", "B2", "B3"]), (5, ["C1", "C2", "C3"]), (6, ["A3", "B3", "C3"]),
        (7, ["D1", "D2", "D3"]), (8, ["E1", "E2", "E3"])
    ]
    threads = [threading.Thread(target=simulate_user, args=(theater, uid, seats)) 
               for uid, seats in users]
    for t in threads: t.start()
    for t in threads: t.join()
    print("\n=== FINAL SEATING CHART ===")
    theater.display_seating_chart()
    theater.analyze_booking_attempts()
    theater.analyze_atomic_violations()

if __name__ == "__main__":
    main()

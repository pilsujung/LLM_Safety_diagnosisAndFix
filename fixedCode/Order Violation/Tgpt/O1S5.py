import asyncio
import random
import time

class EventProcessor:
    def __init__(self):
        self.events = []
        self.results = []

    async def process_event(self, event_id, delay, start_order, my_gate: asyncio.Event, next_gate: asyncio.Event | None):

        prepare_time = time.time()


        await asyncio.sleep(delay)


        finish_time = time.time()


        await my_gate.wait()


        self.results.append({
            'event_id': event_id,
            'prepare_time': prepare_time,
            'finish_time': finish_time,
            'start_order': start_order,
            'delay': delay
        })


        if next_gate is not None:
            next_gate.set()

    async def run_simulation(self, num_events=10):

        gates = [asyncio.Event() for _ in range(num_events)]
        gates[0].set()

        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            my_gate = gates[i]
            next_gate = gates[i + 1] if i + 1 < num_events else None
            task = asyncio.create_task(self.process_event(i, delay, i, my_gate, next_gate))
            self.events.append(task)


        await asyncio.gather(*self.events)


        self.display_results()

    def display_results(self):
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        for processed_order, result in enumerate(self.results):
            order_violation = "Yes" if result['start_order'] != processed_order else "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | "
                  f"{result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {order_violation}")


if __name__ == "__main__":
    processor = EventProcessor()
    asyncio.run(processor.run_simulation())

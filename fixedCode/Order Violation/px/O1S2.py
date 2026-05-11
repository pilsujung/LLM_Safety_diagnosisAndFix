import asyncio
import random
import time

class EventProcessor:
    def __init__(self):
        self.events = []
        self.results = []

    async def process_event(self, event_id, delay, start_order):
        prepare_time = time.time()
        await asyncio.sleep(delay)
        finish_time = time.time()
        self.results.append({
            'event_id': event_id,
            'prepare_time': prepare_time,
            'finish_time': finish_time,
            'start_order': start_order,
            'delay': delay
        })

    async def run_simulation(self, num_events=10):
        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            event = self.process_event(i, delay, i)
            self.events.append(event)
        await asyncio.gather(*self.events)
        self.results.sort(key=lambda x: x['finish_time'])
        self.display_results()

    def display_results(self):
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        processed_order = 0
        for result in self.results:
            order_violation = "Yes" if result['start_order'] != processed_order else "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | {result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {order_violation}")
            processed_order += 1

class OrderFixedEventProcessor(EventProcessor):
    def __init__(self):
        super().__init__()
        self.current_processing = 0
        self.condition = asyncio.Condition()

    async def process_event(self, event_id, delay, start_order):
        prepare_time = time.time()
        await asyncio.sleep(delay)
        async with self.condition:
            await self.condition.wait_for(lambda: start_order == self.current_processing)
            finish_time = time.time()
            self.results.append({
                'event_id': event_id,
                'prepare_time': prepare_time,
                'finish_time': finish_time,
                'start_order': start_order,
                'delay': delay
            })
            self.current_processing += 1
            self.condition.notify_all()

    async def run_simulation(self, num_events=10):
        self.events = []
        self.results = []
        self.current_processing = 0
        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            event = self.process_event(i, delay, i)
            self.events.append(event)
        await asyncio.gather(*self.events)
        self.results.sort(key=lambda x: x['finish_time'])
        self.display_results()

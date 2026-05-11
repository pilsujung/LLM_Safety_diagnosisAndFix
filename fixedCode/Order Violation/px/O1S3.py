import asyncio
import random
import time

class EventProcessor:
    def __init__(self):
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
        tasks = []
        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            task = asyncio.create_task(self.process_event(i, delay, i))
            tasks.append(task)

        await asyncio.gather(*tasks)

        self.results.sort(key=lambda x: x['finish_time'])
        self.display_results()

    def display_results(self):
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        for processed_order, result in enumerate(self.results):
            violation = "Yes" if result['start_order'] != processed_order else "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | "
                  f"{result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {violation}")

async def main():
    processor = EventProcessor()
    await processor.run_simulation()







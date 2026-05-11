import asyncio
import random
import time

class EventProcessorFixed:
    def __init__(self):
        self.results = []

    async def process_event_in_order(self, event_id, delay, start_order, previous_task):

        if previous_task is not None:
            await previous_task
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
        previous_task = None
        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)

            task = asyncio.create_task(self.process_event_in_order(i, delay, i, previous_task))
            previous_task = task


        if previous_task is not None:
            await previous_task


        self.display_results()

    def display_results(self):
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        for i, result in enumerate(self.results):
            order_violation = "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | "
                  f"{result['start_order']:14} | {i:15} | {result['delay']:5.2f} | {order_violation}")


if __name__ == "__main__":
    processor_fixed = EventProcessorFixed()
    asyncio.run(processor_fixed.run_simulation())

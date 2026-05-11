import asyncio
import random
import time

class EventProcessorFixed:
    def __init__(self):
        self.results = []

    async def process_event(self, event_id, delay, start_order):

        prepare_time = time.time()

        await asyncio.sleep(delay)

        finish_time = time.time()

        return {
            'event_id': event_id,
            'prepare_time': prepare_time,
            'finish_time': finish_time,
            'start_order': start_order,
            'delay': delay
        }

    async def run_simulation(self, num_events=10):

        events = []
        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            event = self.process_event(i, delay, i)
            events.append(event)
        

        results = await asyncio.gather(*events)
        

        self.results = sorted(results, key=lambda x: x['start_order'])


        self.display_results()

    def display_results(self):
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        for processed_order, result in enumerate(self.results):

            order_violation = "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | {result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {order_violation}")



class EventProcessorSequential:
    def __init__(self):
        self.results = []
        self.event_queue = asyncio.Queue()

    async def process_event(self, event_id, delay, start_order):

        prepare_time = time.time()

        await asyncio.sleep(delay)

        finish_time = time.time()
        
        return {
            'event_id': event_id,
            'prepare_time': prepare_time,
            'finish_time': finish_time,
            'start_order': start_order,
            'delay': delay
        }

    async def run_simulation_sequential(self, num_events=10):
        print("Sequential Processing (Strict Order):")

        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            result = await self.process_event(i, delay, i)
            self.results.append(result)
        
        self.display_results()

    def display_results(self):
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        for processed_order, result in enumerate(self.results):
            order_violation = "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | {result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {order_violation}")



async def main():
    print("=== Fixed Version (Concurrent but Ordered Results) ===")
    processor_fixed = EventProcessorFixed()
    await processor_fixed.run_simulation()
    
    print("\n=== Sequential Version (Strict Order) ===")
    processor_sequential = EventProcessorSequential()
    await processor_sequential.run_simulation_sequential()


asyncio.run(main())
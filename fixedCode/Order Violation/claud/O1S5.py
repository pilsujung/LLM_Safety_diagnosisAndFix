import asyncio
import random
import time

class EventProcessorFixed:
    def __init__(self):
        self.results = []

    async def process_event(self, event_id, delay, start_order):

        prepare_time = time.time()
        print(f" {event_id}  ...")
        

        await asyncio.sleep(delay)
        

        finish_time = time.time()
        

        result = {
            'event_id': event_id,
            'prepare_time': prepare_time,
            'finish_time': finish_time,
            'start_order': start_order,
            'delay': delay
        }
        self.results.append(result)
        print(f" {event_id}  ")
        return result

    async def run_simulation(self, num_events=10):
        print("   ...")
        

        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)

            await self.process_event(i, delay, i)
        
        print("\n   !")
        

        self.display_results()

    def display_results(self):
        print("\n===   ===")
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        print("-" * 100)
        
        processed_order = 0
        for result in self.results:

            order_violation = "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | {result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {order_violation}")
            processed_order += 1



class EventProcessorQueue:
    def __init__(self):
        self.results = []
        self.completion_queue = asyncio.Queue()

    async def process_event(self, event_id, delay, start_order):

        prepare_time = time.time()
        print(f" {event_id}  ...")
        

        await asyncio.sleep(delay)
        

        finish_time = time.time()
        

        result = {
            'event_id': event_id,
            'prepare_time': prepare_time,
            'finish_time': finish_time,
            'start_order': start_order,
            'delay': delay
        }
        

        await self.completion_queue.put((start_order, result))
        print(f" {event_id}  ")

    async def result_collector(self, num_events):
        """   """
        collected_results = {}
        expected_order = 0
        
        for _ in range(num_events):
            order, result = await self.completion_queue.get()
            collected_results[order] = result
            

            while expected_order in collected_results:
                self.results.append(collected_results.pop(expected_order))
                expected_order += 1

    async def run_simulation_concurrent(self, num_events=10):
        print("  +    ...")
        

        events = []
        for i in range(num_events):
            delay = random.uniform(0.1, 1.0)
            event = self.process_event(i, delay, i)
            events.append(event)
        

        collector = self.result_collector(num_events)
        

        await asyncio.gather(collector, *events)
        
        print("\n      !")
        

        self.display_results()

    def display_results(self):
        print("\n===   ===")
        print("Event ID | Prepare Time | Finish Time | Original Order | Processed Order | Delay | Order Violation")
        print("-" * 100)
        
        processed_order = 0
        for result in self.results:

            order_violation = "No"
            print(f"{result['event_id']:8} | {result['prepare_time']:13.4f} | {result['finish_time']:12.4f} | {result['start_order']:14} | {processed_order:15} | {result['delay']:5.2f} | {order_violation}")
            processed_order += 1


async def main():
    print("===  1:   (Java join()  ) ===")
    processor1 = EventProcessorFixed()
    await processor1.run_simulation(5)
    
    print("\n\n===  2:   +   (Java CountDownLatch  ) ===")
    processor2 = EventProcessorQueue()
    await processor2.run_simulation_concurrent(5)


if __name__ == "__main__":
    asyncio.run(main())
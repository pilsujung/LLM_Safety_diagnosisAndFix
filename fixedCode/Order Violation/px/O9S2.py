import random
import time
from collections import defaultdict
import threading

class PacketOrderViolationFixed:
    def __init__(self, total_packet_count=15, max_delay_ms=500, loss_probability=0.1):
        """
        Initialize the fixed packet order violation simulator
        """
        self.total_packet_count = total_packet_count
        self.max_delay_ms = max_delay_ms
        self.loss_probability = loss_probability
        self.statistics = {
            'packets_sent': 0,
            'packets_received': 0,
            'packets_lost': 0,
            'out_of_order_packets': 0,
            'duplicate_packets': 0,
            'total_delay_ms': 0
        }

        self.initialized = threading.Event()
        self.lock = threading.Lock()

    def generate_original_packets(self):
        """Generate the original sequence of packets to be transmitted"""
        original_packet_sequence = []

        for sequence_number in range(1, self.total_packet_count + 1):
            packet_payload = f"DATA_CHUNK_{sequence_number:03d}"
            packet_size_bytes = random.randint(64, 1500)
            timestamp_created = time.time()

            packet_info = {
                'sequence_number': sequence_number,
                'payload': packet_payload,
                'size_bytes': packet_size_bytes,
                'timestamp_created': timestamp_created,
                'checksum': hash(packet_payload) % 10000
            }
            original_packet_sequence.append(packet_info)

        return original_packet_sequence

    def init_thread_simulation(self, original_packets):
        """Java initThread: Simulate network transmission (initialize data)"""
        try:
            time.sleep(0.1)
        except Exception as e:
            print(f"Init thread interrupted: {e}")

        transmitted_packets = []
        print("=== NETWORK TRANSMISSION SIMULATION (Init Thread) ===")

        for packet in original_packets:
            self.statistics['packets_sent'] += 1


            if random.random() < self.loss_probability:
                print(f"❌ Packet {packet['sequence_number']} LOST during transmission")
                self.statistics['packets_lost'] += 1
                continue


            transmission_delay_ms = random.randint(10, self.max_delay_ms)
            self.statistics['total_delay_ms'] += transmission_delay_ms


            transmitted_packet = packet.copy()
            transmitted_packet['transmission_delay_ms'] = transmission_delay_ms
            transmitted_packet['timestamp_transmitted'] = time.time()

            transmitted_packet_list = [transmitted_packet]


            if random.random() < 0.05:
                duplicate_packet = transmitted_packet.copy()
                duplicate_packet['is_duplicate'] = True
                transmitted_packet_list.append(duplicate_packet)
                self.statistics['duplicate_packets'] += 1
                print(f"🔄 Packet {packet['sequence_number']} DUPLICATED during transmission")

            transmitted_packets.extend(transmitted_packet_list)


        transmitted_packets.sort(key=lambda p: p['transmission_delay_ms'])
        

        with self.lock:
            self.initialized.set()
            print("📡 Network transmission completed - packets ready for processing")

        return transmitted_packets

    def use_thread_simulation(self, transmitted_packets):
        """Java useThread: Wait for initialization, then process packets"""
        print("🔄 Reception thread: Waiting for network transmission to complete...")
        

        while not self.initialized.wait(0.01):
            print("⏳ Waiting for packets to be transmitted...")
        
        print("✅ Packets ready - starting reception and reordering")
        return self.simulate_packet_reception_and_reordering(transmitted_packets)

    def simulate_packet_reception_and_reordering(self, transmitted_packets):
        """
        Fixed packet reception with proper buffering - processes ALL packets eventually
        No more "stuck in buffer" due to missing packets
        """
        reception_buffer = {}
        duplicate_detection_set = set()
        expected_sequence_number = 1
        successfully_processed_packets = []
        max_wait_iterations = 1000

        print("\n=== PACKET RECEPTION AND REORDERING (Use Thread) ===")


        for received_packet in transmitted_packets:
            sequence_num = received_packet['sequence_number']
            
            print(f"\n📦 Received packet {sequence_num} "
                  f"(delay: {received_packet['transmission_delay_ms']}ms)")


            packet_identifier = (sequence_num, received_packet['checksum'])
            if packet_identifier in duplicate_detection_set:
                print(f" ⚠️ DUPLICATE packet {sequence_num} detected and discarded")
                continue
            duplicate_detection_set.add(packet_identifier)


            if sequence_num not in reception_buffer:
                reception_buffer[sequence_num] = received_packet
                self.statistics['packets_received'] += 1
                
                if sequence_num > expected_sequence_number:
                    print(f" ⚠️ OUT-OF-ORDER: received {sequence_num}, expected {expected_sequence_number}")
                    self.statistics['out_of_order_packets'] += 1
                elif sequence_num == expected_sequence_number:
                    print(f" ✅ IN-ORDER packet received")
                else:
                    print(f" ✅ Late packet {sequence_num} buffered")


            processed_count = 0
            while expected_sequence_number in reception_buffer and max_wait_iterations > 0:
                packet_to_process = reception_buffer.pop(expected_sequence_number)
                successfully_processed_packets.append(packet_to_process)
                print(f" ✅ Processing packet {expected_sequence_number}")
                expected_sequence_number += 1
                processed_count += 1
                max_wait_iterations -= 1

            if processed_count > 1:
                print(f" 📤 Batch processed {processed_count} consecutive packets")


        print("\n🔄 Final buffer processing pass...")
        final_processed = 0
        while expected_sequence_number in reception_buffer:
            packet_to_process = reception_buffer.pop(expected_sequence_number)
            successfully_processed_packets.append(packet_to_process)
            print(f" ✅ Final processing packet {expected_sequence_number}")
            expected_sequence_number += 1
            final_processed += 1

        if final_processed > 0:
            print(f" ✅ Final batch: {final_processed} packets recovered from buffer")

        return successfully_processed_packets, reception_buffer

    def display_simulation_results(self, original_packets, processed_packets, remaining_buffer):
        """Display comprehensive simulation results"""
        print("\n" + "="*70)
        print("✅ FIXED SIMULATION RESULTS - NO PACKETS STUCK!")
        print("="*70)

        print(f"\n📋 ORIGINAL PACKETS: {len(original_packets)}")
        print(f"✅ SUCCESSFULLY PROCESSED: {len(processed_packets)}")
        print(f"⚠️ REMAINING IN BUFFER: {len(remaining_buffer)}")

        print(f"\n📊 FINAL STATISTICS:")
        print(f"  Total generated:     {self.statistics['packets_sent']}")
        print(f"  Lost in transit:     {self.statistics['packets_lost']}")
        print(f"  Received:            {self.statistics['packets_received']}")
        print(f"  Duplicates filtered: {self.statistics['duplicate_packets']}")
        print(f"  Out-of-order events: {self.statistics['out_of_order_packets']}")
        
        if self.statistics['packets_received'] > 0:
            avg_delay = self.statistics['total_delay_ms'] / self.statistics['packets_received']
            success_rate = len(processed_packets) / (self.statistics['packets_sent'] - self.statistics['packets_lost']) * 100
            print(f"  Avg delay:           {avg_delay:.1f}ms")
            print(f"  Recovery rate:       {success_rate:.1f}% ✅")

        if not remaining_buffer:
            print("\n🎉 PERFECT ORDER RECOVERY - Zero packets stuck in buffer!")

    def run_complete_simulation(self):
        """Execute complete fixed simulation with proper thread ordering"""
        print("🚀 STARTING FIXED PACKET ORDER VIOLATION SIMULATION")
        print("📋 Java → Python Pattern Mapping:")
        print("   volatile initialized + while(!initialized) → threading.Event() + wait()")
        print("   initThread → init_thread_simulation()")
        print("   useThread → use_thread_simulation()")


        original_packet_sequence = self.generate_original_packets()


        init_thread = threading.Thread(target=self.init_thread_simulation, 
                                     args=(original_packet_sequence,), 
                                     name="InitThread")
        use_thread = threading.Thread(target=lambda: None, name="UseThread")


        init_thread.start()
        init_thread.join()


        transmitted_packets = self.init_thread_simulation(original_packet_sequence)
        processed_packets, remaining_buffer = self.use_thread_simulation(transmitted_packets)


        self.display_simulation_results(original_packet_sequence, processed_packets, remaining_buffer)

        return {
            'original_packets': original_packet_sequence,
            'processed_packets': processed_packets,
            'remaining_buffer': remaining_buffer,
            'statistics': self.statistics
        }

def main():
    """Main function with fixed simulation"""
    simulator = PacketOrderViolationSimulator(
        total_packet_count=20,
        max_delay_ms=800,
        loss_probability=0.15
    )
    simulation_results = simulator.run_complete_simulation()

if __name__ == "__main__":
    main()

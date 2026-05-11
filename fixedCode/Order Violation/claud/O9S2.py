import random
import time
import threading
from collections import defaultdict

class PacketOrderViolationFixed:
    def __init__(self, total_packet_count=15, max_delay_ms=500, loss_probability=0.1):
        """
        Initialize the packet order violation simulator with thread-safe processing
        
        Args:
            total_packet_count: Total number of packets to simulate
            max_delay_ms: Maximum delay in milliseconds for packet transmission
            loss_probability: Probability of packet loss (0.0 to 1.0)
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
        

        self.lock = threading.Lock()
        self.reception_buffer = {}
        self.duplicate_detection_set = set()
        self.expected_sequence_number = 1
        self.successfully_processed_packets = []
        self.transmission_complete = threading.Event()
        self.original_packet_sequence = []
    
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
    
    def transmit_single_packet(self, packet):
        """
        Thread-safe method to transmit a single packet
        Simulates network transmission with potential issues
        """
        with self.lock:
            self.statistics['packets_sent'] += 1
        

        if random.random() < self.loss_probability:
            print(f"❌ Packet {packet['sequence_number']} LOST during transmission")
            with self.lock:
                self.statistics['packets_lost'] += 1
            return None
        

        transmission_delay_ms = random.randint(10, self.max_delay_ms)
        time.sleep(transmission_delay_ms / 1000.0)
        
        with self.lock:
            self.statistics['total_delay_ms'] += transmission_delay_ms
        

        transmitted_packet = packet.copy()
        transmitted_packet['transmission_delay_ms'] = transmission_delay_ms
        transmitted_packet['timestamp_transmitted'] = time.time()
        

        if random.random() < 0.05:
            with self.lock:
                self.statistics['duplicate_packets'] += 1
            print(f"🔄 Packet {packet['sequence_number']} DUPLICATED during transmission")

            threading.Thread(target=self.receive_packet, args=(transmitted_packet.copy(),)).start()
        
        return transmitted_packet
    
    def receive_packet(self, received_packet):
        """
        Thread-safe method to receive and process a single packet
        Ensures proper ordering through synchronization
        """
        if received_packet is None:
            return
        
        with self.lock:
            sequence_num = received_packet['sequence_number']
            
            print(f"\n📦 Received packet {sequence_num} "
                  f"(delay: {received_packet['transmission_delay_ms']}ms)")
            

            packet_identifier = (sequence_num, received_packet['checksum'])
            if packet_identifier in self.duplicate_detection_set:
                print(f"   ⚠️  DUPLICATE packet {sequence_num} detected and discarded")
                return
            self.duplicate_detection_set.add(packet_identifier)
            

            if sequence_num < self.expected_sequence_number:
                print(f"   ❌ Late arrival: packet {sequence_num} already processed")
                return
            elif sequence_num > self.expected_sequence_number:
                print(f"   ⚠️  OUT-OF-ORDER: received {sequence_num}, expected {self.expected_sequence_number}")
                self.statistics['out_of_order_packets'] += 1
            else:
                print(f"   ✅ IN-ORDER packet received")
            

            self.reception_buffer[sequence_num] = received_packet
            self.statistics['packets_received'] += 1
            

            self._process_consecutive_packets()
    
    def _process_consecutive_packets(self):
        """
        Process packets in order from buffer
        Must be called within a locked section
        """
        processed_count = 0
        while self.expected_sequence_number in self.reception_buffer:
            packet_to_process = self.reception_buffer.pop(self.expected_sequence_number)
            self.successfully_processed_packets.append(packet_to_process)
            print(f"   ✅ Processing packet {self.expected_sequence_number}")
            self.expected_sequence_number += 1
            processed_count += 1
        
        if processed_count > 1:
            print(f"   📤 Batch processed {processed_count} consecutive packets")
    
    def simulate_network_transmission(self):
        """
        Simulate concurrent network transmission using threads
        Each packet is transmitted in its own thread to simulate real network behavior
        """
        print("\n=== NETWORK TRANSMISSION SIMULATION (Multi-threaded) ===")
        
        transmission_threads = []
        
        for packet in self.original_packet_sequence:

            def transmit_and_receive(pkt):
                transmitted = self.transmit_single_packet(pkt)
                if transmitted:
                    self.receive_packet(transmitted)
            
            thread = threading.Thread(target=transmit_and_receive, args=(packet,))
            transmission_threads.append(thread)
            thread.start()
            time.sleep(0.01)
        

        for thread in transmission_threads:
            thread.join()
        

        self.transmission_complete.set()
    
    def wait_for_remaining_packets(self, timeout_seconds=2):
        """
        Wait for any remaining packets that might arrive late
        Similar to Java's waiting mechanism
        """
        print(f"\n⏳ Waiting up to {timeout_seconds} seconds for late packets...")
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            with self.lock:
                if not self.reception_buffer:
                    print("✅ All expected packets processed")
                    break
                else:
                    missing = [i for i in range(1, self.expected_sequence_number) 
                              if i not in [p['sequence_number'] for p in self.successfully_processed_packets]]
                    buffered = sorted(self.reception_buffer.keys())
                    print(f"   Waiting for packets. Buffered: {buffered}, Expected: {self.expected_sequence_number}")
            time.sleep(0.1)
    
    def display_simulation_results(self):
        """Display comprehensive simulation results and statistics"""
        
        print("\n" + "="*60)
        print("SIMULATION RESULTS SUMMARY")
        print("="*60)
        

        print("\n📋 ORIGINAL PACKET SEQUENCE:")
        for i, packet in enumerate(self.original_packet_sequence[:8]):
            print(f"   {packet['sequence_number']:2d}. {packet['payload']} "
                  f"({packet['size_bytes']} bytes)")
        if len(self.original_packet_sequence) > 8:
            print(f"   ... and {len(self.original_packet_sequence) - 8} more packets")
        

        with self.lock:
            print(f"\n✅ SUCCESSFULLY PROCESSED PACKETS ({len(self.successfully_processed_packets)}):")
            for packet in self.successfully_processed_packets[:8]:
                print(f"   {packet['sequence_number']:2d}. {packet['payload']} "
                      f"(delay: {packet['transmission_delay_ms']}ms)")
            if len(self.successfully_processed_packets) > 8:
                print(f"   ... and {len(self.successfully_processed_packets) - 8} more packets")
            

            if self.reception_buffer:
                print(f"\n⚠️  UNPROCESSED PACKETS DUE TO ORDER VIOLATIONS ({len(self.reception_buffer)}):")
                for seq_num in sorted(self.reception_buffer.keys()):
                    packet = self.reception_buffer[seq_num]
                    print(f"   {seq_num:2d}. {packet['payload']} "
                          f"(waiting for packet {self.expected_sequence_number})")
            

            print(f"\n📊 TRANSMISSION STATISTICS:")
            print(f"   Total packets generated:     {self.statistics['packets_sent']}")
            print(f"   Packets successfully sent:   {self.statistics['packets_sent'] - self.statistics['packets_lost']}")
            print(f"   Packets lost in transit:     {self.statistics['packets_lost']}")
            print(f"   Packets received:            {self.statistics['packets_received']}")
            print(f"   Duplicate packets detected:  {self.statistics['duplicate_packets']}")
            print(f"   Out-of-order packets:        {self.statistics['out_of_order_packets']}")
            print(f"   Successfully processed:      {len(self.successfully_processed_packets)}")
            print(f"   Packets stuck in buffer:     {len(self.reception_buffer)}")
            

            if self.statistics['packets_received'] > 0:
                avg_delay = self.statistics['total_delay_ms'] / self.statistics['packets_received']
                success_rate = len(self.successfully_processed_packets) / self.statistics['packets_sent'] * 100
                print(f"   Average transmission delay:  {avg_delay:.1f}ms")
                print(f"   End-to-end success rate:     {success_rate:.1f}%")
    
    def run_complete_simulation(self):
        """Execute the complete packet order violation simulation with thread safety"""
        print("🚀 STARTING THREAD-SAFE PACKET ORDER SIMULATION")
        print(f"Configuration: {self.total_packet_count} packets, "
              f"max delay {self.max_delay_ms}ms, {self.loss_probability*100:.1f}% loss probability")
        

        self.original_packet_sequence = self.generate_original_packets()
        

        self.simulate_network_transmission()
        

        self.wait_for_remaining_packets(timeout_seconds=2)
        

        self.display_simulation_results()
        
        return {
            'original_packets': self.original_packet_sequence,
            'processed_packets': self.successfully_processed_packets,
            'remaining_buffer': self.reception_buffer,
            'statistics': self.statistics
        }


def main():
    """Main function to run the thread-safe packet simulation"""

    simulator = PacketOrderViolationFixed(
        total_packet_count=20,
        max_delay_ms=800,
        loss_probability=0.15
    )
    

    simulation_results = simulator.run_complete_simulation()

if __name__ == "__main__":
    main()
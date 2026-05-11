import random
import time
from collections import defaultdict

class PacketOrderViolationSimulator:
    def __init__(self, total_packet_count=15, max_delay_ms=500, loss_probability=0.1):
        """
        Initialize the packet order violation simulator
        
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
    
    def simulate_network_transmission(self, original_packets):
        """
        Simulate network transmission with:
        - Variable delays (for statistics)
        - Packet loss
        - Occasional duplicates
        WITHOUT introducing delivery order violations.
        """
        transmitted_packet_list = []
        
        print("=== NETWORK TRANSMISSION SIMULATION ===")
        
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
            
            transmitted_packet_list.append(transmitted_packet)
            

            if random.random() < 0.05:
                duplicate_packet = transmitted_packet.copy()
                duplicate_packet['is_duplicate'] = True
                transmitted_packet_list.append(duplicate_packet)
                self.statistics['duplicate_packets'] += 1
                print(f"🔄 Packet {packet['sequence_number']} DUPLICATED during transmission")
        

        transmitted_packet_list.sort(key=lambda p: p['sequence_number'])
        
        return transmitted_packet_list
    
    def simulate_packet_reception_and_reordering(self, transmitted_packets):
        """
        Simulate the receiving end attempting to reorder packets
        and handle various network issues
        """
        reception_buffer = {}
        duplicate_detection_set = set()
        expected_sequence_number = 1
        successfully_processed_packets = []
        
        print("\n=== PACKET RECEPTION AND REORDERING SIMULATION ===")
        
        for received_packet in transmitted_packets:
            sequence_num = received_packet['sequence_number']
            
            print(f"\n📦 Received packet {sequence_num} "
                  f"(delay: {received_packet['transmission_delay_ms']}ms)")
            

            packet_identifier = (sequence_num, received_packet['checksum'])
            if packet_identifier in duplicate_detection_set:
                print(f"   ⚠️  DUPLICATE packet {sequence_num} detected and discarded")
                continue
            duplicate_detection_set.add(packet_identifier)
            

            if sequence_num < expected_sequence_number:
                print(f"   ❌ Late arrival: packet {sequence_num} already processed")
                continue
            elif sequence_num > expected_sequence_number:
                print(f"   ⚠️  OUT-OF-ORDER: received {sequence_num}, expected {expected_sequence_number}")
                self.statistics['out_of_order_packets'] += 1
            else:
                print(f"   ✅ IN-ORDER packet received")
            

            reception_buffer[sequence_num] = received_packet
            self.statistics['packets_received'] += 1
            

            processed_count = 0
            while expected_sequence_number in reception_buffer:
                packet_to_process = reception_buffer.pop(expected_sequence_number)
                successfully_processed_packets.append(packet_to_process)
                print(f"   ✅ Processing packet {expected_sequence_number}")
                expected_sequence_number += 1
                processed_count += 1
            
            if processed_count > 1:
                print(f"   📤 Batch processed {processed_count} consecutive packets")
        
        return successfully_processed_packets, reception_buffer
    
    def display_simulation_results(self, original_packets, processed_packets, remaining_buffer):
        """Display comprehensive simulation results and statistics"""
        
        print("\n" + "="*60)
        print("SIMULATION RESULTS SUMMARY")
        print("="*60)
        

        print("\n📋 ORIGINAL PACKET SEQUENCE:")
        for i, packet in enumerate(original_packets[:8]):
            print(f"   {packet['sequence_number']:2d}. {packet['payload']} "
                  f"({packet['size_bytes']} bytes)")
        if len(original_packets) > 8:
            print(f"   ... and {len(original_packets) - 8} more packets")
        

        print(f"\n✅ SUCCESSFULLY PROCESSED PACKETS ({len(processed_packets)}):")
        for packet in processed_packets[:8]:
            print(f"   {packet['sequence_number']:2d}. {packet['payload']} "
                  f"(delay: {packet['transmission_delay_ms']}ms)")
        if len(processed_packets) > 8:
            print(f"   ... and {len(processed_packets) - 8} more packets")
        

        if remaining_buffer:
            print(f"\n⚠️  UNPROCESSED PACKETS DUE TO ORDER VIOLATIONS ({len(remaining_buffer)}):")
            for seq_num in sorted(remaining_buffer.keys()):
                packet = remaining_buffer[seq_num]
                print(f"   {seq_num:2d}. {packet['payload']} "
                      f"(waiting for packet {min(remaining_buffer.keys()) - 1 if remaining_buffer else 'N/A'})")
        

        print(f"\n📊 TRANSMISSION STATISTICS:")
        print(f"   Total packets generated:     {self.statistics['packets_sent']}")
        print(f"   Packets successfully sent:   {self.statistics['packets_sent'] - self.statistics['packets_lost']}")
        print(f"   Packets lost in transit:     {self.statistics['packets_lost']}")
        print(f"   Packets received:            {self.statistics['packets_received']}")
        print(f"   Duplicate packets detected:  {self.statistics['duplicate_packets']}")
        print(f"   Out-of-order packets:        {self.statistics['out_of_order_packets']}")
        print(f"   Successfully processed:      {len(processed_packets)}")
        print(f"   Packets stuck in buffer:     {len(remaining_buffer)}")
        

        if self.statistics['packets_received'] > 0:
            avg_delay = self.statistics['total_delay_ms'] / self.statistics['packets_received']
            success_rate = len(processed_packets) / self.statistics['packets_sent'] * 100
            print(f"   Average transmission delay:  {avg_delay:.1f}ms")
            print(f"   End-to-end success rate:     {success_rate:.1f}%")
    
    def run_complete_simulation(self):
        """Execute the complete packet order violation simulation"""
        print("🚀 STARTING PACKET ORDER VIOLATION SIMULATION")
        print(f"Configuration: {self.total_packet_count} packets, "
              f"max delay {self.max_delay_ms}ms, {self.loss_probability*100:.1f}% loss probability")
        

        original_packet_sequence = self.generate_original_packets()
        

        transmitted_packets = self.simulate_network_transmission(original_packet_sequence)
        

        processed_packets, remaining_buffer = self.simulate_packet_reception_and_reordering(transmitted_packets)
        

        self.display_simulation_results(original_packet_sequence, processed_packets, remaining_buffer)
        
        return {
            'original_packets': original_packet_sequence,
            'processed_packets': processed_packets,
            'remaining_buffer': remaining_buffer,
            'statistics': self.statistics
        }


def main():
    """Main function to run the packet order violation simulation"""

    simulator = PacketOrderViolationSimulator(
        total_packet_count=20,
        max_delay_ms=800,
        loss_probability=0.15
    )
    

    simulation_results = simulator.run_complete_simulation()

if __name__ == "__main__":
    main()

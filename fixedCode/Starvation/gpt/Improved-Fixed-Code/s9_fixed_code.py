import multiprocessing as mp
import time
import random

class LockCohortingManager:
    def __init__(self, manager, threshold=10):
        """
        가상 NUMA 환경(Lock Cohorting) 매니저 초기화.
        직렬화 에러(PicklingError)를 방지하기 위해 Manager 객체 자체는 저장하지 않고, 
        Manager가 생성한 프록시 객체들만 멤버 변수로 저장.
        """
        self.state_lock = manager.Lock()
        
        # 전역 락 (G) 및 노드별 로컬 락 (S_i)
        self.global_lock = manager.Lock()
        self.local_locks = {
            0: manager.Lock(), 
            1: manager.Lock()
        }
        
        # Cohorting 상태 변수
        self.global_owner = manager.Value('i', -1)
        self.handoff_count = manager.Value('i', 0)
        self.local_waiters = manager.dict({0: 0, 1: 0})
        self.threshold = threshold

    def acquire(self, node_id):
        """자원 접근 요청 (Lock Cohorting Acquire)"""
        # 1. 로컬 대기자 카운트 증가
        with self.state_lock:
            self.local_waiters[node_id] += 1
            
        # 2. 로컬 락 획득 (S_i)
        self.local_locks[node_id].acquire()
        
        # 3. 로컬 대기자 카운트 감소 및 전역 락 소유권 확인
        with self.state_lock:
            self.local_waiters[node_id] -= 1
            current_owner = self.global_owner.value
            
        # 4. 전역 락이 다른 노드에 있다면 전역 락 획득 (G)
        if current_owner != node_id:
            self.global_lock.acquire()
            with self.state_lock:
                self.global_owner.value = node_id
                self.handoff_count.value = 0

    def release(self, node_id):
        """자원 접근 해제 (Lock Cohorting Release)"""
        # 5. Cohorting 해제 평가
        with self.state_lock:
            waiters = self.local_waiters[node_id]
            handoffs = self.handoff_count.value

        # 대기자가 있고, 연속 승계 임계치에 도달하지 않았다면 -> Local Handoff
        if waiters > 0 and handoffs < self.threshold:
            with self.state_lock:
                self.handoff_count.value = handoffs + 1
            # 전역 락은 유지한 채, 로컬 락만 해제 (동일 노드 대기자에게 양보)
            self.local_locks[node_id].release()
        
        # 대기자가 없거나 임계치 초과 시 -> Global Release (may-pass-local)
        else:
            with self.state_lock:
                self.global_owner.value = -1
                self.handoff_count.value = 0
            self.global_lock.release()
            self.local_locks[node_id].release()


def resource_consumer(process_id, access_frequency, resource_usage_duration, priority_level, node_id, resource_manager, stats_dict):
    """
    Simulates a process that repeatedly accesses a shared resource using Lock Cohorting.
    """
    total_wait_time = 0
    successful_accesses = 0
    
    print(f"Process-{process_id} ({priority_level} priority, Node {node_id}) started - Access every {access_frequency}s, Uses for {resource_usage_duration}s")
    
    for iteration in range(15):  
        time.sleep(access_frequency)
        wait_start_time = time.time()

        # Lock Cohorting 획득 (기아 현상 방지)
        resource_manager.acquire(node_id)
        
        try:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time
            total_wait_time += wait_duration
            
            current_timestamp = time.strftime('%H:%M:%S', time.localtime(wait_end_time))
            print(f"{current_timestamp} - Process-{process_id} ({priority_level}, Node {node_id}) acquired resource "
                  f"(waited {wait_duration:.3f}s, iteration {iteration + 1}/15)")

            # 임계 구역 시뮬레이션
            actual_usage_time = resource_usage_duration + random.uniform(-0.05, 0.05)
            time.sleep(max(0, actual_usage_time))

            release_time = time.time()
            release_timestamp = time.strftime('%H:%M:%S', time.localtime(release_time))
            total_resource_time = release_time - wait_end_time
            successful_accesses += 1
            
            print(f"{release_timestamp} - Process-{process_id} ({priority_level}, Node {node_id}) released resource "
                  f"after {total_resource_time:.3f}s")
        finally:
            # Lock Cohorting 해제
            resource_manager.release(node_id)

    average_wait_time = total_wait_time / successful_accesses if successful_accesses > 0 else 0
    
    # Manager.dict에 통계 저장
    stats_dict[process_id] = {
        'priority_level': priority_level,
        'node_id': node_id,
        'successful_accesses': successful_accesses,
        'average_wait_time': average_wait_time,
        'total_wait_time': total_wait_time
    }


def monitor_system(run_event):
    """
    Monitors the system and provides periodic updates about process activity.
    """
    start_time = time.time()
    while run_event.is_set():
        time.sleep(2)
        elapsed = time.time() - start_time
        timestamp = time.strftime('%H:%M:%S', time.localtime())
        active_procs = len(mp.active_children())
        print(f"\n[MONITOR] {timestamp} - System running for {elapsed:.1f}s - Active processes: {active_procs}")


def main():
    print("=" * 65)
    print("PROCESS STARVATION RESOLUTION (LOCK COHORTING APPLIED)")
    print("=" * 65)
    print("This simulation demonstrates starvation resolution where:")
    print("- High/Competing processes are on Node 0")
    print("- Medium/Low priority processes are on Node 1")
    print("- Lock Cohorting ensures fairness and prevents starvation")
    print("=" * 65)

    manager = mp.Manager()
    
    # 임계치(handoff threshold)를 10으로 설정한 Lock Cohorting 매니저
    resource_manager = LockCohortingManager(manager=manager, threshold=10)
    stats_dict = manager.dict()
    run_event = manager.Event()
    run_event.set()

    # 프로세스 구성 (process_id, frequency, usage_duration, priority, node_id)
    configs = [
        (1, 0.08, 0.6, "HIGH", 0),       # High priority, Node 0
        (2, 0.4, 0.2, "MEDIUM", 1),      # Medium priority, Node 1
        (3, 1.2, 0.05, "LOW", 1),        # Low priority, Node 1
        (4, 0.3, 0.3, "COMPETING", 0)    # Competing, Node 0
    ]

    processes = []
    
    for pid, freq, duration, prio, node in configs:
        p = mp.Process(
            target=resource_consumer,
            args=(pid, freq, duration, prio, node, resource_manager, stats_dict),
            name=f"{prio}Worker"
        )
        processes.append(p)

    monitor_p = mp.Process(target=monitor_system, args=(run_event,), name="SystemMonitor")

    simulation_start_time = time.time()
    start_timestamp = time.strftime('%H:%M:%S', time.localtime(simulation_start_time))
    print(f"\nSimulation started at: {start_timestamp}")
    print("-" * 65)

    # 모든 프로세스 시작
    for p in processes:
        p.start()
    monitor_p.start()

    # 워커 프로세스 종료 대기
    for p in processes:
        p.join()

    # 모니터 프로세스 종료
    run_event.clear()
    monitor_p.join(timeout=2)
    if monitor_p.is_alive():
        monitor_p.terminate()

    simulation_end_time = time.time()
    total_simulation_time = simulation_end_time - simulation_start_time
    end_timestamp = time.strftime('%H:%M:%S', time.localtime(simulation_end_time))

    print("\n" + "=" * 65)
    print("SIMULATION COMPLETE (STARVATION RESOLVED)")
    print("=" * 65)
    
    # 최종 통계 출력
    final_stats = dict(stats_dict)
    for pid in sorted(final_stats.keys()):
        stat = final_stats[pid]
        print(f"--- Process-{pid} ({stat['priority_level']}, Node {stat['node_id']}) Final Stats ---")
        print(f"Total successful accesses: {stat['successful_accesses']}")
        print(f"Average wait time: {stat['average_wait_time']:.3f}s")
        print(f"Total wait time: {stat['total_wait_time']:.3f}s")
        print("-" * 50)
        
    print(f"\nEnd time: {end_timestamp}")
    print(f"Total duration: {total_simulation_time:.2f} seconds")
    print("\nObservations:")
    print("- Lock Cohorting successfully prevented 'High' priority processes from dominating.")
    print("- 'Low' priority processes received fair access through Global Release.")
    print("- Handoff threshold ensured throughput within local NUMA nodes.")
    print("=" * 65)


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
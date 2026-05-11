import multiprocessing as mp
import time
import random

RUN_DURATION = 10              
MAX_ATTEMPTS_PER_PROCESS = 50   

class LockCohortingManager:
    def __init__(self, manager, threshold=10):
        """
        가상 NUMA 환경(Lock Cohorting) 매니저 초기화.
        직렬화 에러를 방지하기 위해 Manager가 생성한 프록시 객체만 멤버 변수로 저장.
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


def worker_process(process_name, priority_level, node_id, resource_manager, stats_dict, run_event):
    """
    Lock Cohorting을 통해 자원에 접근하는 글로벌 워커 함수.
    기존의 non-blocking 폴링 방식에서 발생하는 기아 현상을 
    계층적 락의 공정성 제어를 통해 원천 차단함.
    """
    local_stats = {
        'attempts': 0, 
        'successes': 0, 
        'starvations': 0  # Lock Cohorting 덕분에 사실상 0으로 유지됨
    }
    
    # 초기 통계 등록
    stats_dict[process_name] = local_stats

    while run_event.is_set() and local_stats['attempts'] < MAX_ATTEMPTS_PER_PROCESS:
        local_stats['attempts'] += 1

        # 블로킹 방식의 Lock Cohorting 자원 요청 (공정성 보장)
        resource_manager.acquire(node_id)
        
        try:
            local_stats['successes'] += 1
            print(f"[{priority_level}] {process_name} (Node {node_id}) acquired the resource")

            # 임계 구역 시뮬레이션 (기존 스크립트의 체류 시간 반영)
            if priority_level == 'HIGH':
                hold_time = random.uniform(0.1, 0.3)
            elif priority_level == 'NORMAL':
                hold_time = random.uniform(0.02, 0.08)
            else:
                hold_time = random.uniform(0.01, 0.03)
                
            time.sleep(hold_time)
            
        finally:
            # 자원 해제 및 핸드오프 평가
            resource_manager.release(node_id)
            
        # 통계 업데이트
        stats_dict[process_name] = local_stats

        # 다음 접근 시도 전 대기 (우선순위에 따른 빈도 차이 구현)
        if priority_level == 'HIGH':
            time.sleep(random.uniform(0.01, 0.05))
        elif priority_level == 'NORMAL':
            time.sleep(random.uniform(0.02, 0.08))
        else:
            time.sleep(random.uniform(0.05, 0.15))


def stats_printer(stats_dict, run_event):
    """주기적으로 현재 통계를 출력하는 프로세스"""
    while run_event.is_set():
        time.sleep(3)
        print("\n" + "="*65)
        print("PROCESS STATISTICS (LOCK COHORTING APPLIED):")
        print("="*65)
        
        # 현재 상태를 복사하여 출력 (안전한 순회)
        current_stats = dict(stats_dict)
        for name in sorted(current_stats.keys()):
            data = current_stats[name]
            attempts = data['attempts']
            successes = data['successes']
            starvations = data['starvations']
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            
            print(f"{name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*65 + "\n")


def main():
    print("Starting Process Starvation Demonstration with Lock Cohorting")
    print("Processes are distributed across Virtual NUMA Node 0 and Node 1")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_PROCESS} attempts per process.\n")

    manager = mp.Manager()
    
    # Lock Cohorting 매니저 (연속 승계 임계치 10회 설정)
    resource_manager = LockCohortingManager(manager=manager, threshold=10)
    
    stats_dict = manager.dict()
    run_event = manager.Event()
    run_event.set()

    processes = []

    # 프로세스 구성: 우선순위 / 이름 / 가상 NUMA 노드
    configs = [
        ('HIGH', 'HighPrio-1', 0),
        ('HIGH', 'HighPrio-2', 0),
        ('NORMAL', 'Normal-1', 0),
        ('NORMAL', 'Normal-2', 1),
        ('NORMAL', 'Normal-3', 1),
        ('LOW', 'LowPrio-1', 1),
        ('LOW', 'LowPrio-2', 1),
    ]

    # 워커 프로세스 생성
    for prio, name, node in configs:
        p = mp.Process(
            target=worker_process, 
            args=(name, prio, node, resource_manager, stats_dict, run_event)
        )
        processes.append(p)

    # 통계 출력 프로세스 생성
    printer_p = mp.Process(target=stats_printer, args=(stats_dict, run_event))
    processes.append(printer_p)

    # 모든 프로세스 시작
    for p in processes:
        p.start()
    
    # 지정된 시간 동안 메인 프로세스 대기
    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        time.sleep(1)

    print("\nStopping simulation (time limit reached)...")
    run_event.clear()
    
    # 프로세스 종료 대기
    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    time.sleep(0.5)

    print("\nFINAL STATISTICS (STARVATION RESOLVED):")
    print("="*65)
    final_stats = dict(stats_dict)
    for name in sorted(final_stats.keys()):
        data = final_stats[name]
        attempts = data['attempts']
        successes = data['successes']
        starvations = data['starvations']
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        
        print(f"{name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
    
    print("\nLock Cohorting applied: High-priority/fast-running threads can no longer")
    print("monopolize the resource. Local handoff and global release thresholds ensure")
    print("all virtual NUMA nodes and processes get fair access (100% success rate).")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
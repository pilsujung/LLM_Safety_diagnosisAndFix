import multiprocessing as mp
import time

class ResourceAllocator:
    def __init__(self, manager, threshold=10):
        """
        가상 NUMA 환경(Lock Cohorting) 매니저 초기화.
        직렬화(Pickling) 에러 방지를 위해 Manager가 생성한 프록시 객체만 저장.
        """
        # 상태 보호용 락
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

        # 기존 s5.py의 상태 관리 및 로깅용 프록시
        self.access_count = manager.dict({
            'priority_thread_0': 0,
            'priority_thread_1': 0,
            'starved_thread_0': 0,
            'starved_thread_1': 0
        })
        self.resource_usage_log = manager.list()

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
            # 전역 락은 유지한 채, 로컬 락만 해제 (동일 노드의 대기자에게 양보)
            self.local_locks[node_id].release()
            
        # 대기자가 없거나 임계치 초과 시 -> Global Release (may-pass-local)
        else:
            with self.state_lock:
                self.global_owner.value = -1
                self.handoff_count.value = 0
            self.global_lock.release()
            self.local_locks[node_id].release()

def worker_process(thread_name, node_id, allocator, target_count, work_time, sleep_time):
    """
    가상 NUMA 노드에 할당되어 Lock Cohorting을 이용해 자원에 접근하는 글로벌 워커 함수
    """
    count = 0
    while count < target_count:
        # Cohorting 방식으로 락 획득 (기아 현상 방지)
        allocator.acquire(node_id)
        try:
            # 임계 구역 (Critical Section)
            current_count = allocator.access_count[thread_name] + 1
            allocator.access_count[thread_name] = current_count
            
            log_msg = f"{thread_name} (Node {node_id}) accessed at count {current_count}"
            allocator.resource_usage_log.append(log_msg)
            print(f"{thread_name} (Node {node_id}) accessed resource {current_count} times")
            
            time.sleep(work_time)
            count += 1
        finally:
            # Cohorting 방식으로 락 해제
            allocator.release(node_id)
            
        # 경쟁 상황을 모사하기 위한 대기
        time.sleep(sleep_time)

def simulate_starvation_resolution():
    manager = mp.Manager()
    
    # 임계치(handoff threshold) = 10회 설정
    allocator = ResourceAllocator(manager=manager, threshold=10)

    processes = []
    
    # 가상 NUMA 노드 0 그룹: 우선순위가 높아 자원을 독점하던 성향의 프로세스 모음
    for i in range(2):
        name = f'priority_thread_{i}'
        p = mp.Process(target=worker_process, args=(name, 0, allocator, 50, 0.01, 0.005))
        processes.append(p)

    # 가상 NUMA 노드 1 그룹: 기아 현상을 겪던(starved) 성향의 프로세스 모음
    for i in range(2):
        name = f'starved_thread_{i}'
        p = mp.Process(target=worker_process, args=(name, 1, allocator, 50, 0.01, 0.02))
        processes.append(p)

    print("Lock Cohorting 시뮬레이션 시작 (기아 현상 방지 테스트)...")
    
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    print("\n최종 접근 카운트 (공정성 확보 결과):")
    for k, v in dict(allocator.access_count).items():
        print(f"  {k}: {v}")

    # 기록된 로그 중 일부 출력 (너무 길어지는 것을 방지)
    print("\n리소스 사용 로그 (처음 10개 & 마지막 10개):")
    logs = list(allocator.resource_usage_log)
    for log in logs[:10]:
        print(log)
    print("...")
    for log in logs[-10:]:
        print(log)

if __name__ == "__main__":
    # Windows 호환성 및 직렬화 안전성을 위한 spawn 방식 강제
    mp.set_start_method('spawn', force=True)
    simulate_starvation_resolution()
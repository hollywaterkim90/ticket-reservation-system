# 🚀 Kafka 대용량 예매 트래픽 처리 프로젝트

본 프로젝트는 대규모 동시성 예매 요청 환경을 가정하고, **Kafka 배치 컨슈머의 비동기 스레드 풀 격리 구조**와 **Redis 분산 캐시 기반의 멱등성 방어벽**을 구축하여 극한의 장애 상황 속에서도 시스템 정합성을 어떻게 유지하는지 증명하기 위한 아키텍처 검증 가이드입니다.

---

## 🏗️ 1. 인프라스트럭처 및 토픽 토폴로지 구성

고가용성(HA)과 분산 처리를 보장하기 위해 3개의 브로커(Kafka Cluster) 환경에서 Partition과 Replication Factor를 각각 3으로 구성하여 병렬 처리 및 고하중 처리가 가능하도록 토픽을 구성합니다.

### 토픽 (Partitions: 3, Replication Factor: 3)
```bash
1. ticket-reservations
2. ticket-payments
3. ticket-reservations.DLQ
```


### 부하 테스트
```bash
#!/bin/bash
echo "🔥 대용량 예매 부하 테스트 시작 (10000건 동시 요청 비동기 쉘 발사)..."

// Mac or linux
for i in {1..5000}; do curl -X POST "http://localhost:8085/reserve" -H "Content-Type: application/json" -d "{\"userId\":\"user$i\", \"ticketId\":\"ticket:stock:god\"}" & done wait
echo "✅ 10000건의 분산 요청 발사 완료. 컨슈머 리스너 메트릭 확인 필요."

//  Windows
for /L %i in (1,1,5000) do curl -X POST "http://localhost:8085/reserve" -H "Content-Type: application/json" -d "{\"userId\":\"user%i\", \"ticketId\":\"ticket:stock:god\"}"
```


### 프로젝트 진행 중 개선사항

⚡ 비동기 스레드 풀 격리 (Thread Pool Isolation)
대량 분산 환경에서의 컨슈머 블로킹을 방지하기 위해, Kafka 메인 리스너 스레드와 결제 워커 스레드를 분리했습니다. CompletableFuture.supplyAsync와 고정 스레드 풀(paymentExecutor)을 조합하여 대량의 요청을 병렬 처리합니다.

🛡️ Redis 분산 멱등성 락 (Idempotency Barrier)
비동기 결제 로직 최상단에서 Redis 내 order:status:{orderId} 상태를 선점 조회합니다. 이미 처리 완료된 주문(SUCCESS/FAILURE)은 외부 PG사 API 연동을 타지 않고 즉시 스킵(⏭️ 멱등성 블로킹)시킵니다.

🚨 장애 격리 (Dead Letter Queue)
비즈니스 결함(예: 잔액 부족 유저 user300)이나 타임아웃이 발생한 악성 메세지는 파이프라인을 오염시키지 않도록 ticket-reservations.DLQ 토픽으로 안전하게 격리 배송하여 보상 트랜잭션을 유도합니다.
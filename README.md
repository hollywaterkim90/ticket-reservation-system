# 🚀 Kafka 대용량 예매 트래픽 처리 프로젝트

본 프로젝트는 대규모 동시성 예매 요청 환경을 가정하고, **Kafka 배치 컨슈머의 비동기 스레드 풀 격리 구조**와 **Redis 분산 캐시 기반의 멱등성 방어벽**을 구축하여 극한의 장애 상황 속에서도 시스템 정합성을 어떻게 유지하는지 증명하기 위한 아키텍처 검증 가이드입니다.

---

## ⚡ 빠른 시작 (로컬 실행 순서)

로컬 minikube에서 **처음부터 끝까지** 따라 하는 순서입니다. 명령은 **저장소 루트**에서 실행합니다.

> **준비물:** Docker, `minikube`, `kubectl`, (KEDA 설치용) `helm`
> **폴더 구조:** 쿠버네티스 매니페스트는 `infra/`, 실행 스크립트는 `scripts/`, 각 서비스 소스와 `Dockerfile`은 `ticket-*-service/` 안에 있습니다.

### STEP 1. minikube 시작
넉넉한 리소스로 클러스터를 띄웁니다. (ES·Kibana·Kafka 3대까지 올라가므로 메모리를 크게 잡습니다)
```bash
minikube start --driver=docker --cpus=4 --memory=8192
```

### STEP 2. KEDA 설치 (최초 1회만)
Kafka lag 기반 오토스케일링에 필요합니다. 이미 설치돼 있으면 건너뜁니다.
```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace

# 설치 확인 (CRD 가 보이면 완료)
kubectl get crd scaledobjects.keda.sh
```

### STEP 3. 애플리케이션 이미지 빌드 → minikube 로드
**스크립트는 이미지를 빌드하지 않습니다.** 배포 전에 두 서비스 이미지를 만들어 minikube 안으로 넣어야 합니다. (로컬 docker로 빌드한 이미지는 minikube가 자동으로 못 보기 때문)
```bash
# 예매 서비스
cd ticket-reservation-service
docker build -t ticket-reservation-service:latest .
minikube image load ticket-reservation-service:latest
cd ..

# 결제 서비스
cd ticket-payment-service
docker build -t ticket-payments-service:latest .
minikube image load ticket-payments-service:latest
cd ..
```

### STEP 4. 배포 + 로컬 접속 스크립트 실행
`infra/` 의 매니페스트를 모두 apply하고, 롤아웃을 기다린 뒤, 로컬 접속용 `port-forward`까지 자동으로 띄웁니다.
```powershell
# Windows (PowerShell)
.\scripts\start-local.ps1
```
```bash
# Mac / Linux
chmod +x scripts/start-local.sh scripts/stop-local.sh   # 최초 1회
./scripts/start-local.sh
```
실행이 끝나면 접속 가능:  **Kafka UI → http://localhost:8090** ,  **예매 API → http://localhost:8085/reserve**

### STEP 5. curl 로 과부하 테스트
5000건의 예매 요청을 발사합니다. (요청은 위 스크립트가 열어 둔 `localhost:8085` 로 들어갑니다)
```bash
# Mac / Linux (비동기 동시 발사)
for i in {1..5000}; do curl -s -X POST "http://localhost:8085/reserve" -H "Content-Type: application/json" -d "{\"userId\":\"user$i\", \"ticketId\":\"ticket:stock:god\"}" & done; wait
```
```cmd
:: Windows (cmd, 순차 발사)
for /L %i in (1,1,5000) do curl -s -X POST "http://localhost:8085/reserve" -H "Content-Type: application/json" -d "{\"userId\":\"user%i\", \"ticketId\":\"ticket:stock:god\"}"
```

### STEP 6. 결과 관찰
부하로 `ticket-reservations` lag이 임계치(30)를 넘으면 결제 서비스 팟이 **1 → 3** 으로 오토스케일됩니다.
```bash
kubectl get hpa -w                                    # lag / replica 실시간
kubectl get pods -l app=ticket-payments-service -w    # 팟 스케일아웃
```
- **Kafka UI** ( http://localhost:8090 ) 에서 토픽별 메시지/컨슈머 lag 을 눈으로 확인할 수 있습니다.

### STEP 7. 정리
```powershell
# Windows: port-forward 터널 정리
.\scripts\stop-local.ps1
```
```bash
# Mac / Linux: port-forward 터널 정리
./scripts/stop-local.sh
```
```bash
# 쿠버네티스 리소스까지 내리려면
kubectl delete -f infra/
# 클러스터 전체 종료
minikube stop
```

> 각 단계의 **원리·상세 옵션·트러블슈팅**(port-forward가 왜 필요한지, 좀비 터널 정리, KEDA 동작 원리 등)은 아래 **"2. 쿠버네티스(minikube) 배포 및 로컬 접속"** 섹션에 정리돼 있습니다.

---

## 🏗️ 1. 인프라스트럭처 및 토픽 토폴로지 구성

고가용성(HA)과 분산 처리를 보장하기 위해 3개의 브로커(Kafka Cluster) 환경에서 Partition과 Replication Factor를 각각 3으로 구성하여 병렬 처리 및 고하중 처리가 가능하도록 토픽을 구성합니다.

### 토픽 (Partitions: 3, Replication Factor: 3)
```bash
1. ticket-reservations
2. ticket-payments
3. ticket-reservations.DLQ
```


---

## ☸️ 2. 쿠버네티스(minikube) 배포 및 로컬 접속

### 2-0. 한 번에 실행 (권장)

배포(apply) → 롤아웃 대기 → 로컬 접속용 `port-forward`까지 한 번에 처리하는 스크립트를 제공합니다. OS에 맞는 것을 사용하세요.

```powershell
# Windows (PowerShell) — 저장소 루트에서 실행
.\scripts\start-local.ps1     # 배포 + port-forward 실행
.\scripts\stop-local.ps1      # port-forward 터널만 정리
```
```bash
# Mac / Linux — 저장소 루트에서 실행
chmod +x scripts/start-local.sh scripts/stop-local.sh   # 최초 1회
./scripts/start-local.sh      # 배포 + port-forward 실행
./scripts/stop-local.sh       # port-forward 터널만 정리
```

실행 후 접속: **Kafka UI → http://localhost:8090** , **예매 API → http://localhost:8085/reserve**

> **왜 스크립트인가?** `port-forward`는 쿠버네티스 리소스가 아니라 *내 PC에서 도는 클라이언트 프로세스*라서 `kubectl apply`(YAML)에 넣을 수 없습니다. 그래서 apply 이후 스크립트가 백그라운드로 port-forward를 띄우고, 그 PID를 `.local-pf-pids`에 기록해 `stop` 스크립트가 정리합니다.
>
> 아래 2-1 ~ 2-3 은 이 스크립트가 내부적으로 수행하는 단계를 수동으로 풀어 쓴 것입니다(원리 이해/디버깅용).

### 2-1. 서비스 배포 (이미지 빌드 → minikube 로드 → apply)

각 서비스에 `Dockerfile`(멀티스테이지 빌드)이 포함되어 있습니다. **로컬 docker로 빌드한 이미지는 minikube가 바로 보지 못하므로, 빌드 후 `minikube image load`로 밀어넣어야 합니다.**

```powershell
# (저장소 루트에서 실행. 쿠버네티스 매니페스트는 infra/ 폴더에 모여 있음)

# 1) 인프라(Kafka 3대 / Redis / ES / Kibana / Kafka-UI) 및 토픽 생성
kubectl apply -f infra/infra.yaml

# 2) 예매 서비스 (Producer, 외부 8085 → 내부 8080)
#    Dockerfile 은 각 서비스 폴더에 있으므로 빌드는 그 폴더로 이동해서 수행
cd ticket-reservation-service
docker build -t ticket-reservation-service:latest .
minikube image load ticket-reservation-service:latest
cd ..
kubectl apply -f infra/ticket-reservation-depl.yaml

# 3) 결제 서비스 (Consumer, 8080)
cd ticket-payment-service
docker build -t ticket-payments-service:latest .
minikube image load ticket-payments-service:latest
cd ..
kubectl apply -f infra/ticket-payment-depl.yaml

# 4) KEDA 오토스케일 (Kafka lag 기반 HPA)
kubectl apply -f infra/ticket-payments-keda-broker-alias.yaml   # keda 네임스페이스용 Kafka 별칭(아래 설명)
kubectl apply -f infra/ticket-payments-keda.yaml
```

> **이미지 수정 후 재배포 시:** `docker build` → `minikube image load` → `kubectl rollout restart deploy/<이름>` 순서로 진행하세요. (`imagePullPolicy: IfNotPresent`라 rollout restart로 새 이미지를 다시 읽게 해야 합니다.)

### 2-2. 로컬에서 접속하기 (port-forward)

**왜 필요한가:** minikube를 **docker 드라이버**로 실행하면 쿠버네티스 `NodePort`가 minikube 컨테이너 *내부*에만 열리고 Windows 호스트의 `localhost`로는 직접 뚫리지 않습니다. 그래서 `localhost:30003` 같은 접속이 안 됩니다.

`kubectl port-forward`는 **내 PC의 `localhost:포트` → 클러스터 안 서비스로 연결하는 임시 터널**입니다. 아래 명령을 **각각 별도의 PowerShell 창**에서 실행하세요. (실행하면 그 창은 터널이 살아있는 동안 계속 점유됩니다. `Ctrl+C`로 종료.)

```powershell
# Kafka UI  → 브라우저에서 http://localhost:8090
kubectl port-forward svc/kafka-ui 8090:8090

# 예매 서비스(부하 테스트용) → http://localhost:8085
kubectl port-forward svc/ticket-reservation-service 8085:8085
```

| 대상 | 접속 주소 | 비고 |
|---|---|---|
| Kafka UI | http://localhost:8090 | port-forward 필요 |
| 예매(reserve) API | http://localhost:8085 | port-forward 필요 (부하 스크립트가 사용) |

> **대안:** `minikube service kafka-ui` 를 쓰면 터널을 자동으로 뚫고 브라우저까지 열어줍니다. 다만 포트가 **랜덤**으로 배정되므로, 부하 스크립트처럼 **고정 포트(8085)가 필요할 땐 위의 `port-forward`** 를 사용하세요.

### 2-3. 접속이 안 될 때 (port-forward 트러블슈팅)

port-forward는 터널이 끊기거나, 프로세스가 **좀비**로 남아 포트만 붙잡고 전달은 안 하는 경우가 있습니다. 이럴 땐 해당 포트를 잡은 프로세스를 정리 후 재실행하세요.

```powershell
netstat -ano | findstr :8090   # 8090을 잡고 있는 PID 확인
taskkill /PID <PID> /F         # 좀비 프로세스 종료
kubectl port-forward svc/kafka-ui 8090:8090   # 다시 실행
```

### 2-4. KEDA 오토스케일 관찰

부하가 들어와 `ticket-reservations` 토픽의 컨슈머 lag이 임계치(**30**)를 넘으면, 결제 서비스 팟이 **1 → 3**으로 오토스케일됩니다. (파티션 3개 = 최대 컨슈머 3개)

```powershell
kubectl get hpa -w                                    # lag/replica 실시간 관찰
kubectl get pods -l app=ticket-payments-service -w    # 팟 스케일아웃 관찰

# 결제 컨슈머 그룹의 active consumer 수 확인 (평소 1 → 부하 시 최대 3)
kubectl exec <kafka-1 팟 이름> -- kafka-consumer-groups `
  --bootstrap-server localhost:9092 --describe --group ticket-group-payment-worker --members
```

> **설계 노트 ①** 팟당 컨슈머 스레드는 `KAFKA_CONSUMER_CONCURRENCY=1`(deployment env)로 **1개**입니다. 스케일아웃은 앱 스레드가 아니라 **KEDA가 팟 수를 늘려서** 담당합니다. 그래야 "평소 active consumer 1 → lag 발생 시 증가"가 성립합니다.
>
> 토픽의 **파티션 수(3)** 와는 다른 값입니다. 파티션 수는 컨슈머 병렬도의 *상한*(파티션 N개 → 컨슈머 최대 N개)이고, `KAFKA_CONSUMER_CONCURRENCY`는 팟 하나가 띄우는 스레드 수입니다.
>
> **설계 노트 ②** KEDA operator는 `keda` 네임스페이스에서 도는데, Kafka 브로커가 advertised.listener로 짧은 이름(`kafka-1-service`)을 반환합니다. 이 이름은 `keda` 네임스페이스에서 해석되지 않으므로, `infra/ticket-payments-keda-broker-alias.yaml`이 동일 이름의 `ExternalName` 서비스를 만들어 `default`의 실제 브로커로 별칭 연결합니다. (Kafka 재시작 없이 해결)

---

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
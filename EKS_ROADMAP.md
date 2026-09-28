# EKS 로드맵 — 티켓 예매 시스템을 EKS로

## 📍 지금 여기 (2026-09-29 갱신)

> **작업 시작할 때 이 세 줄만 읽으면 된다. 끝낼 때 이 세 줄만 고친다.**

- **끝남**: Budgets 알림 / IAM 준비 / 로컬 검증(예매는 Redis 만으로 뜬다, ES env 삭제 가능) / 브랜치 3개 푸시
- **다음 한 걸음**: **ECR 저장소 생성 + 이미지 푸시** (`docker build --platform linux/amd64`, 태그는 커밋 해시)
- **그다음**: Terraform 스켈레톤(`terraform/`, provider, `.gitignore`) → **여기까지 하고 이사 끝날 때까지 동결**
- **보류**: 1단계 이후 전부(이사 후 재개) / 플링크(첫 슬라이스로 종료) / PR 열기 / 이슈 #40·#42 수정

**페이스 결정(2026-09-29)**: 지원 재개가 2027 1~2월이므로 **10~11월에 못 끝내도 아무 일도 안 생긴다.**
0단계만 닫고 멈춘다. 1단계부터는 `apply`/`destroy` 반복이라 집중이 필요해 이사 준비와 겹치면 손해다.

---

작성 2026-09-18. 목적은 포트폴리오가 아니라 **클라우드 실무 능력(VPC·IAM·스토리지·LB·비용)** 채우기.
새 프로젝트를 만들지 않고 **기존 티켓 시스템을 쓴다.** 상태 저장(PVC)이 있어야 진짜 문제가 생기기 때문.

**범위 (2026-09-18 A안 확정)**: **예매 서비스 + Redis** 로 시작 → 2단계에서 **Kafka 1대** 추가.
결제 서비스·Postgres·ES·Kibana·Redis Insight·Kafka UI 는 올리지 않는다.
- 근거: 예매는 Postgres 를 안 쓴다(의존 = Redis 기동 필수, Kafka 는 요청 시점). 결제는 Postgres+Kafka+ES 를 다 요구(`TicketIndexListener`).
- PVC 는 Kafka 가 가져온다 → 2단계 EBS CSI(IRSA)가 억지 없이 필요해진다.

**왜 — 이미 겪은 문제가 관리형에서는 다른 방식으로 풀린다**
- [#31](../../issues/31) 유실을 막으려 넣은 Outbox 의 저장소가 `emptyDir` 이던 모순 → **EBS(PVC)** 로 풀린다
- [#33](../../issues/33) `minikube image load` 는 같은 태그를 덮지 않아 옛 이미지가 조용히 계속 돌던 문제 → **레지스트리(ECR) + 커밋 해시 태그**로 풀린다

> 이 문서는 절대 기준이 아니다. 코드를 확인해 어긋나면 고친다.

---

## A. 매니페스트 수정 목록

### 안 고치면 안 뜨는 것

| # | 위치 | 지금 | EKS에서 | 바꿀 것 |
|---|---|---|---|---|
| 1 | `infra/ticket-reservation-depl.yaml:18-19` | `image: ticket-reservation-service:latest`, `IfNotPresent` | ImagePullBackOff | ECR 주소 + **커밋 해시 태그**(`:latest` 금지) |
| 1-1 | 이미지 빌드(맥) | Apple Silicon 기본 빌드 = arm64 | 노드가 amd64면 `exec format error` | **`docker build --platform linux/amd64`** — 아래 0-1 |
| 2 | `infra/volumes.yaml` kafka-1-data | `storageClassName` 없음 | PVC **Pending** (2단계) | **EBS CSI Driver(→IRSA)** + StorageClass |
| 3 | `infra/ticket-reservation-depl.yaml:30-42` | Kafka 3대 주소 + ES 주소 주입 | Kafka 1대 구성과 불일치 | Kafka 주소를 1대로, ES env 삭제(의존성만 있고 안 씀 — 로컬에서 확인) |
| 3-1 | `infra/infra.yaml:163-` kafka-1 + `:428` init-kafka-topics Job | **3대 전제**: quorum voters 3개, offsets RF 3, min.insync 2, 토픽 RF 3, Job 이 3대 다 기다림 | 1대면 토픽 생성·쓰기 실패 | voters 1개, RF 1, min.insync 1, Job 대기 대상 1대 |
| 3-2 | `infra/infra.yaml` kafka-1 `KAFKA_LOG_DIRS: /tmp/kraft-combined-logs` + 볼륨 마운트 | **이미지에 없는 경로** → 볼륨이 root 소유로 생성 | EBS 붙이면 `appuser`(uid 1000)가 못 씀 → `meta.properties.tmp (Permission denied)` 로 CrashLoop | 경로를 **`/var/lib/kafka/data`**(이미지 기본, appuser 소유)로, 또는 파드에 `securityContext.fsGroup`. **2026-09-22 로컬 docker-compose 에서 실제로 겪음** — minikube 는 프로비저너가 느슨해서 넘어갔던 것 |
| 4 | `infra/ticket-reservation-depl.yaml:66-71` | `NodePort: 30085` | 프라이빗 서브넷이라 외부 접근 불가 | `LoadBalancer` + **AWS LB Controller(→IRSA)** |

### 안 고쳐도 뜨지만 면접에서 묻는 것

| # | 위치 | 문제 |
|---|---|---|
| 5 | ~~`infra/infra.yaml:487-491` DB 비밀번호 평문~~ | A안에선 Postgres 를 안 올려서 **해당 없음**. Secret 은 필요해지면 그때 |
| 6 | 전체 | `resources` 요청·제한 없음 → 노드 크기 산정 불가 = 비용. **requests 는 1단계로 당김(아래 1-1)**, limits 는 4단계 |
| 7 | `infra/infra.yaml:163` Kafka `Deployment`+RWO PVC | 노드가 **다른 AZ**면 EBS 못 붙음 → StatefulSet/토폴로지 제약 |
| 8 | `infra/ticket-reservation-depl.yaml:60-64` | actuator 없음 → **LB 헬스체크 경로** 없음 |

---

## B. 단계

| 단계 | 내용 | 나오는 결과물 |
|---|---|---|
| **0. 준비** | Budgets 알림 → IAM 준비 → ECR 저장소+이미지 푸시(**`--platform linux/amd64`**) → Terraform 스켈레톤 | 비용 감시 |
| **1. public EKS + 앱** | Terraform VPC(퍼블릭/프라이빗) + EKS(public endpoint) + 노드그룹은 프라이빗. **예매+Redis 만**, 수정 1 적용, **requests 넣고 노드 산정(1-1)** | 기준선 "앱은 뜬다" (예매 요청은 Kafka 없어 실패 — 정상) + 추정치 기록 |
| **2. Kafka + IRSA** ★ | Kafka 1대 추가(수정 3·3-1) → PVC Pending 을 만남 → EBS CSI(수정 2), AWS LB Controller(수정 4), **metrics-server** (`kubectl top` 으로 실사용량) | "파드마다 IAM 을 왜 쪼개나" 설명 가능 |
| **3. 조이기** ★★ 본체 | 3a NAT 제거→VPC 엔드포인트(ECR 풀 실패→**S3 Gateway** 필요) / 3b EKS 엔드포인트 Public→Public+Private→**Private only**(kubectl 끊김→bastion) | 깨진 기록 = 글① 재료 |
| **4. 운영 요소** | **Prometheus + Grafana**(`kube-prometheus-stack` Helm 차트 하나에 Prometheus·Grafana·Alertmanager 포함), **limits 조정 — 1단계 추정치 vs 실측 대조(6)**, AZ 묶임(7), **EKS 버전 업그레이드 1회**(아래 4-1) | 공고 빈칸(Prometheus/Grafana) 해소. 시선에이아이 자격요건 |
| **5. 비용·정리** | `terraform destroy` 실패 겪기(ELB·EBS 잔여 ENI) → k8s 리소스 먼저 삭제 → 재시도. 며칠치 실측 | 글②③ 재료 |
| **6. 글 3개** | ①온프레미스 폐쇄망 ↔ EKS fully private 비교(최대 차별화) ②destroy 함정 ③비용 실측 | 본체 |

### 0-1. 아키텍처 선택 — amd64 (2026-09-22 확정)
**노드 `t3.medium`(amd64) + 이미지 `--platform linux/amd64`.** 이유: 실무 표준이 아직 x86.
- ⚠️ 맥(Apple Silicon)에서 그냥 빌드하면 **arm64** 가 나와 파드가 `exec format error` 로 죽는다. 빌드마다 `--platform linux/amd64` 필수. 에뮬레이션이라 5~10분.
- 검증 `docker image inspect <img> --format '{{.Architecture}}'` → `amd64`
- 참고: 쓰는 이미지(cp-kafka 7.7.0·redis 7.2-alpine·temurin 17-jre·gradle 8.10)는 **전부 arm64 도 지원**한다(2026-09-22 확인). 비용(t4g 약 20% 저렴)·빌드 속도로 Graviton 전환은 언제든 가능 — 인스턴스 타입 한 줄.

### 1-1. resources requests (2026-09-22 앞당김)
limits 와 분리한다. **requests = 스케줄링 기준 = 노드 크기 = 비용**이라 노드 타입을 고르기 전에 있어야 한다.
- 1단계: 파드별 requests 를 **추정으로 넣고, 그 합으로 노드 타입·대수를 정한다.** 추정 근거를 문서에 남길 것(이게 나중에 대조 대상).
- 2단계: metrics-server 애드온 → `kubectl top pod/node` 로 실사용량 확인.
- 4단계: Prometheus 값과 대조 → limits 조정. **"처음 얼마로 잡았고, 실제 얼마였고, 왜 틀렸나"가 글③(비용)의 본체.**
- ⚠️ 4단계에 몰면 추정이 없어서 이 비교 자체가 생기지 않는다.

### 4-1. EKS 버전 업그레이드 (2026-09-18 추가)
공고 40건 중 25건이 k8s "구축·운영 경험"을 필수로 요구. 온프레미스 경험은 **단일 노드**라 멀티 노드 운영(drain·재스케줄·PDB) 경험이 빈칸.
- 1단계부터 **표준 지원 중인 버전 중 최신보다 하나 낮은 버전**으로 만든다 → 4단계에서 한 단계만 올린다(마이너 버전은 한 번에 하나씩).
  - ⚠️ **확장 지원(extended support) 버전으로 만들지 말 것** → 컨트롤플레인 요금이 $0.10 → $0.60/h(6배).
- 순서: 컨트롤플레인 → 애드온(VPC CNI·CoreDNS·kube-proxy·EBS CSI) 호환 확인 → 매니지드 노드그룹 롤링
- 관찰할 것: Kafka 파드가 drain 될 때 PVC 가 다른 AZ 노드로 못 따라가는지(수정 7과 연결), PDB 가 없으면 무슨 일이 생기는지
- 글감: **온프레미스 단일 노드 1.28 마이그레이션 ↔ EKS 멀티 노드 롤링 업그레이드**

**반복 규칙**: `apply` → 작업 → **`destroy`**. 단계 끝날 때마다 내린다.

---

## C. 일정 (주 5시간 기준 — 평일 저녁 1회 + 주말 1회, 각 2~2.5시간)

| 단계 | 예상 시간 | 목표 시기 |
|---|---|---|
| 0. 준비 | 3~4h | 9월 4주 |
| 1. public EKS + 앱 | 6~8h | 10월 1~2주 |
| 2. Kafka + IRSA | 4~6h | 10월 3주 |
| 3. 조이기 | 8~10h | 10월 4주 ~ 11월 2주 |
| 4. 운영 요소 (+업그레이드 3~4h) | 7~10h | 11월 3~4주 |
| 5. 비용·정리 | 3~4h | 11월 4주~12월 초 |
| 6. 글 3개 | 6~9h | 12월 이후 / 이사 끝나고 |
| **합계** | **37~51h** | |

- **12월은 비워둔다(이사).** 각 단계가 독립적이라 중간에 끊겨도 잃는 게 없다.
- 지원 재개는 2027년 1~2월(사내대출 정리 후)이므로 **일정에 여유가 있다.**
- 막히면 그 단계에 시간을 더 쓰되 **범위는 늘리지 않는다.** Terraform CI/CD·멀티환경·Atlantis 는 하지 않는다.

## D. 예상 비용

올려둔 시간에만 과금된다. 총 가동 40시간 기준:

| 항목 | 단가 | 40h |
|---|---|---|
| EKS 컨트롤플레인 | $0.10/h (프리티어 없음) | $4 |
| 노드 t3.medium ×2 | 약 $0.083/h | $3~4 |
| NAT 게이트웨이 | 약 $0.045/h + 데이터 | $2~3 |
| ELB, EBS, ECR | 소액 | $1~3 |
| **합계** | | **약 $10~15 (1.5~2만원)** |

**가장 큰 비용 위험은 지우는 걸 잊는 것.** 그래서 0단계의 Budgets 알림을 먼저 건다.

---

## 다음 한 걸음

1. **AWS Budgets 알림 걸기** (5분, 무료)
2. ~~1단계 범위 결정~~ → A안 확정(2026-09-18)
3. **로컬 확인**: 예매 서비스가 **Redis 만 있고 Kafka·ES 없이** 기동되는지 (ES env 를 지워도 되는지 근거)

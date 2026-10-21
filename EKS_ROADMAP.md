# EKS 로드맵 — 티켓 예매 시스템을 EKS로

## 📍 지금 여기 (2026-10-22 갱신)

> **작업 시작할 때 이 세 줄만 읽으면 된다. 끝낼 때 이 세 줄만 고친다.**

- **끝남**: Budgets 알림 / IAM 준비 / 로컬 검증(예매는 Redis 만으로 뜬다, ES env 삭제 가능) / 브랜치 3개 병합 / **ECR 저장소 생성 + 이미지 푸시 (2026-10-05)**
  - `846456671266.dkr.ecr.ap-northeast-2.amazonaws.com/ticket-reservation-service:15e122c` (amd64/linux, 약 164MB)
  - **1단계 매니페스트 (2026-10-18)** — `infra/eks/{01-redis,02-reservation}.yaml`. 원본 `infra/*.yaml` 은 보존(minikube 비교 서사 유지). ClusterIP + `port-forward`(비용 0), requests 추정값 입력
  - **`terraform/eks.tf` (2026-10-18)** — 모듈 `~> 21.0`, public 엔드포인트, `authentication_mode=API`, t3.medium 2대
  - **Terraform 스켈레톤 (2026-10-16)** — `terraform/{versions,provider,variables}.tf` + `.gitignore`. `init`·`validate`·`plan` 통과(No changes). 리소스는 0개.
- **✅ 0단계 완료.** 1단계 착수: **`terraform/vpc.tf` 작성 완료**(validate 통과, `apply` 는 안 함 → 비용 0).
- **✅ 1단계 코드 완료**(VPC·EKS·매니페스트). **다음 한 걸음 = `terraform apply` 이고 이사 끝난 뒤 2027 1월 이후.** 여기서부터 과금 시작.
  - apply 후: `aws eks update-kubeconfig` → `kubectl apply -f infra/eks/` → `port-forward` 로 기동 확인 → **requests 추정치 기록**(1-1)
- **✅ 2단계 Kafka 매니페스트 완료 (2026-10-20, #48 / PR #49)** — `infra/eks/03-kafka-storage.yaml`(StorageClass `ebs-gp3` + PVC(PersistentVolumeClaim)) · `04-kafka.yaml`(1대, 수정 3-1·`fsGroup: 1000`) · `02-reservation.yaml`(bootstrap 1대 주소)
  - ⚠️ 정리 순서: PVC 먼저 삭제 → `terraform destroy`. 클러스터부터 허물면 Delete 가 실행되지 않아 EBS(Elastic Block Store) 볼륨이 남아 과금
- **▶ 다음 한 걸음 = 수정 2: EBS CSI(Container Storage Interface) 드라이버 + IRSA(IAM Roles for Service Accounts), Terraform (#51)** — `validate`·`plan` 까지, apply 없음
- **1단계 시작 시 확정된 값**: 리전 `ap-northeast-2` / **EKS `1.36`**(최신 1.37 보다 N-1, 표준 지원 2027-08-02 까지. 1.35 는 2027-03-27 종료라 밀리면 확장 지원=요금 6배) / 노드 `t3.medium`(amd64) / 태그 `Project=ticket-eks`
- **1단계는 사용자가 직접 작성하고 Claude 가 리뷰한다** — VPC 서브넷 구조·노드 수·엔드포인트 설정은 전부 면접 질문거리([[ai-assisted-learning-rule]] 기준)
- **보류**: `apply` 이후 전부(이사 후 재개, 코드 작성은 지금 진행) / 플링크(첫 슬라이스로 종료, #41 클로즈) / 이슈 #39·#40 수정

**페이스 결정(2026-09-29, 2026-10-19 갱신)**: 지원은 **2027.8 아내 복직 이후**(잔금 전엔 사내대출 때문에 불가), 서류 준비는 2027 상반기이므로 **10~11월에 못 끝내도 아무 일도 안 생긴다.**
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
| **1. public EKS + 앱** | ✅VPC 작성됨(`vpc.tf`). Terraform VPC(퍼블릭/프라이빗) + EKS(public endpoint) + 노드그룹은 프라이빗. **예매+Redis 만**, 수정 1 적용, **requests 넣고 노드 산정(1-1)** | 기준선 "앱은 뜬다" (예매 요청은 Kafka 없어 실패 — 정상) + 추정치 기록 |
| **2. Kafka + IRSA** ★ | Kafka 1대 추가(수정 3·3-1) → PVC Pending 을 만남 → EBS CSI(수정 2), AWS LB Controller(수정 4), **metrics-server**(`kubectl top`), **권한 3층 실습(2-1)** | "파드마다 IAM 을 왜 쪼개나" 설명 가능 |
| **3. 조이기** ★★ 본체 | 3a NAT 제거→VPC 엔드포인트(ECR 풀 실패→**S3 Gateway** 필요) / 3b EKS 엔드포인트 Public→Public+Private→**Private only**(kubectl 끊김→bastion) | 깨진 기록 = 글① 재료 |
| **4. 운영 요소** | **Prometheus + Grafana**(`kube-prometheus-stack` Helm 차트 하나에 Prometheus·Grafana·Alertmanager 포함), **limits 조정 — 1단계 추정치 vs 실측 대조(6)**, AZ 묶임(7), **EKS 버전 업그레이드 1회**(4-1), **GPU 노드 + vLLM 서빙(4-2)** | 공고 빈칸(Prometheus/Grafana·GPU) 해소 |
| **5. 비용·정리** | `terraform destroy` 실패 겪기(ELB·EBS 잔여 ENI) → k8s 리소스 먼저 삭제 → 재시도. 며칠치 실측 | 글②③ 재료 |
| **6. 글 3개** | ①온프레미스 폐쇄망 ↔ EKS fully private 비교(최대 차별화) ②destroy 함정 ③비용 실측 | 본체 |

### 0-2. ⚠️ 도커 데몬이 둘이면 빌드와 푸시가 엇갈린다 (2026-10-05 실제로 겪음)
맥에 **Docker Desktop + Colima** 두 데몬이 있었다. 빌드는 성공하고 로그에 `naming to ...:15e122c done` 까지 찍혔는데
`docker image inspect` 는 `No such image` → 이미지는 **Colima 데몬 안**, 셸은 **Docker Desktop** 을 보고 있었다.
- 확인: `docker context ls` (별표가 현재), `docker --context <이름> image ls`
- **ECR 로그인도 데몬별로 따로 해야 한다.** `aws ecr get-login-password | docker --context colima login ...`
- **이슈 #33(`minikube image load` 가 같은 태그를 안 덮던 문제)과 같은 종류** — "이미지가 실제로 어디에 있는가".
- 2026-10-05 이후 **Docker Desktop 은 쓰지 않고 Colima 하나로 통일**한다 → `docker context use colima`

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

### 2-1. 권한 3층 실습 — IRSA / Access Entries / k8s RBAC (2026-10-18 추가)
IRSA 만으로는 절반이다. **AWS 권한(IRSA)과 클러스터 내부 권한(k8s RBAC)은 다른 층.**
| 층 | 무엇 | 할 일 |
|---|---|---|
| ① 노드그룹 IAM 역할 | 노드가 ECR 풀·CNI IP 할당 | 1단계에 포함 |
| ② **IRSA** | **파드 단위 IAM** = 최소권한의 클라우드 구현 | EBS CSI·LB Controller 로 이미 함 |
| ③ **Access Entries**(옛 aws-auth) | IAM 사용자·역할 → k8s 로 매핑 | **두 번째 IAM 사용자를 추가해 `kubectl` 권한 부여**(클러스터는 `admin` 으로 생성됨) |
| ④ **k8s RBAC** | ServiceAccount / Role·ClusterRole / Binding | **읽기 전용 ClusterRole 하나 만들어 붙여보기** (30분~1h, 비용 0) |
💼 **업무 연결**: 백업·DR 제품(Veeam Kasten 류)은 **클러스터 전체를 읽는 ClusterRole** 을 요구한다 → 보안 검토에서 반드시 걸리는 지점이고 사용자가 기여할 자리. ④를 직접 해보면 "어디까지 주고 어디서 끊나"를 말할 수 있다.

### 4-3. PVC 스냅샷 → 복원 (DR 연습) (2026-10-18 추가)
**왜**: 회사에서 **재해복구(DR) 솔루션 커스터마이징 프로젝트**에 참여 예정(k8s 적용 가능성 높음). 2~3h, **비용 거의 0**(스냅샷 저장료 몇 센트).
대상은 **Kafka PVC**(이미 있음). EBS CSI(2단계) 의 연장이다.
| 겪는 것 | DR 개념 |
|---|---|
| `VolumeSnapshotClass` + 스냅샷 생성 | 백업 시점 = **RPO**(Recovery Point Objective) |
| 스냅샷 → PVC 복원 → 파드 재기동 | 복구 시간 = **RTO**(Recovery Time Objective) 체감 |
| **다른 AZ 로 복원 시도** | **사이트 전환의 축소판.** AZ 묶임과 또 충돌 |
| 복원 후 Kafka 데이터 확인 | **일관성**(crash-consistent vs application-consistent) |
→ 글감: "스냅샷은 떴는데 복원이 안 되는 이유"(StorageClass·AZ·StatefulSet 재바인딩).

### 4-2. GPU 노드 + vLLM 서빙 (2026-10-17 추가)
**왜**: AI 인프라·플랫폼 간판의 유일한 빈 칸이 GPU(공고 40건 중 71% 언급). vLLM 은 대부분 "우대".
**구성**: `g4dn.xlarge`(NVIDIA T4 16GB) **1대, 스팟**(약 $0.2/h / 온디맨드 약 $0.6/h). 작업할 때만 올린다. **비용 +$5~10**(6~10h).

할 일
1. Terraform 에 **GPU 노드그룹**(스팟 1대) 추가
2. **NVIDIA device plugin** 또는 **GPU Operator** — k8s 가 GPU 를 자원(`nvidia.com/gpu`)으로 인식
3. **vLLM 디플로이먼트** — 작은 모델(0.5B~3B). T4 16GB 로 충분
4. Service 노출 → 추론 요청
5. 측정: **토큰/초, 응답 지연, GPU 메모리**
6. **DCGM exporter → Prometheus**(4단계에서 이미 띄움) → GPU 메트릭 수집. **DCGM 은 공고 반복 키워드**

⚠️ **예고된 함정 = 글감 — 모델 가중치 반입**
평소엔 vLLM 이 기동 시 외부(HuggingFace)에서 받는다. **3단계로 조인 뒤/폐쇄망에서는 실패한다.**
→ **이미지 반입 문제와 같은 구조**(사용자 실무와 동일). 해법: 미리 받아 **PVC** / **S3** / 이미지에 포함 — 트레이드오프가 다르다.
💡 **글 ④ 후보: "폐쇄망에서 LLM 을 서빙하려면 모델 가중치를 어떻게 반입하나."** LLM 서빙하는 사람은 대개 클라우드에서 하고, 폐쇄망 하는 사람은 대개 LLM 을 안 올린다. 둘을 걸친 글은 드물다.
💰 모델 파일이 수 GB → **NAT 로 받으면 데이터 전송 요금.** 큰 모델은 S3 에 올려 두고 **S3 게이트웨이 엔드포인트**(3단계 산물)로 받으면 전송료 0.

### 4-1. EKS 버전 업그레이드 (2026-09-18 추가)
공고 40건 중 25건이 k8s "구축·운영 경험"을 필수로 요구. 온프레미스 경험은 **단일 노드**라 멀티 노드 운영(drain·재스케줄·PDB) 경험이 빈칸.
- 1단계부터 **표준 지원 중인 버전 중 최신보다 하나 낮은 버전**으로 만든다 → 4단계에서 한 단계만 올린다(마이너 버전은 한 번에 하나씩).
  - ⚠️ **확장 지원(extended support) 버전으로 만들지 말 것** → 컨트롤플레인 요금이 $0.10 → $0.60/h(6배).
- 순서: 컨트롤플레인 → 애드온(VPC CNI·CoreDNS·kube-proxy·EBS CSI) 호환 확인 → 매니지드 노드그룹 롤링
- ⚠️ **`max_size` 를 먼저 올릴 것** — 매니지드 노드그룹 업그레이드는 **새 노드를 띄우고 옛 노드를 비운다.**
  1단계 설정이 `desired=2, max_size=2` 라 **추가로 띄울 여유가 없어 업그레이드가 느려지거나 막힌다.**
  → 업그레이드 전 `max_size` 를 3으로 올리고, 끝나면 되돌린다(비용 상한 유지).
- 관찰할 것: Kafka 파드가 drain 될 때 PVC 가 다른 AZ 노드로 못 따라가는지(수정 7과 연결), PDB 가 없으면 무슨 일이 생기는지
- 글감: **온프레미스 단일 노드 1.28 마이그레이션 ↔ EKS 멀티 노드 롤링 업그레이드**

**반복 규칙**: `apply` → 작업 → **`destroy`**. 단계 끝날 때마다 내린다.

---

## C. 일정 (주 5시간 기준 — 평일 저녁 1회 + 주말 1회, 각 2~2.5시간)

| 단계 | 예상 시간 | 목표 시기 |
|---|---|---|
| 0. 준비 | 3~4h | 9월 4주 |
| 1. public EKS + 앱 | 6~8h | 10월 1~2주 |
| 2. Kafka + IRSA (+권한 3층 1~2h) | 5~8h | 2027년 2월 |
| 3. 조이기 | 8~10h | 10월 4주 ~ 11월 2주 |
| 4. 운영 요소 (+업그레이드 3~4h, +GPU·vLLM 6~10h, +스냅샷 DR 2~3h) | 15~23h | 2027년 5월 |
| 5. 비용·정리 | 3~4h | 11월 4주~12월 초 |
| 6. 글 3개 | 6~9h | 12월 이후 / 이사 끝나고 |
| **합계** | **46~67h** (비용 2.5~3만원) | |

- **12월은 비워둔다(이사).** 각 단계가 독립적이라 중간에 끊겨도 잃는 게 없다.
- 지원은 2027년 8월(아내 복직 이후), 서류 준비는 2027 상반기이므로 **일정에 여유가 있다.**
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

# EKS 전체 구성도

> 기준: 2026-11-02, `terraform/` + `infra/eks/01~05`. apply 전이라 "코드가 만들 모습"이다.

## 그림

```
                              인터넷 (맥에서 curl / kubectl)
                                   │
                          ┌────────┴────────┐
                          │ Internet Gateway │
                          └────────┬────────┘
┌─ VPC 10.0.0.0/16 ────────────────┼──────────────────────────────────────────┐
│                                  │                                           │
│     AZ ap-northeast-2a           │            AZ ap-northeast-2c             │
│  ┌──────────────────────────┐    │    ┌──────────────────────────┐          │
│  │ 퍼블릭 10.0.1.0/24        │    │    │ 퍼블릭 10.0.2.0/24        │          │
│  │                          │    ▼    │                          │          │
│  │   ┌──── ALB (Gateway API 로 생성, internet-facing, 두 AZ) ──┐   │          │
│  │   └────────────────────────┬──────────────────────────────┘   │          │
│  │   NAT 게이트웨이 (1개뿐)    │                                   │          │
│  └──────────▲────────────────┼──┘    └───────────────────────────┘          │
│             │ 아웃바운드      │ /reserve 만 → 파드 IP:8080 (target-type: ip)  │
│  ┌──────────┴────────────────▼──┐    ┌──────────────────────────────┐       │
│  │ 프라이빗 10.0.11.0/24         │    │ 프라이빗 10.0.12.0/24         │       │
│  │                              │    │                              │       │
│  │  노드 t3.medium              │    │  노드 t3.medium              │       │
│  │   ├ 예매 서비스 파드?         │    │   ├ 예매 서비스 파드?         │       │
│  │   ├ Redis 파드?              │    │   ├ Redis 파드?              │       │
│  │   ├ Kafka 파드? ── EBS gp3   │    │   ├ Kafka 파드? ── EBS gp3   │       │
│  │   └ kube-system:             │    │   └ kube-system:             │       │
│  │     LB Controller, EBS CSI,  │    │     (같은 종류가 분산)        │       │
│  │     CoreDNS, VPC CNI ...     │    │                              │       │
│  │  EKS 컨트롤플레인 ENI         │    │  EKS 컨트롤플레인 ENI         │       │
│  └──────────────────────────────┘    └──────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────────────┘

  VPC 밖 (AWS 관리형):  EKS 컨트롤플레인 본체 · ECR(이미지) · STS/IAM(IRSA) · EBS
```

`?` = 파드가 어느 노드(AZ)에 뜰지는 스케줄러가 정한다. 지정하지 않았다.

## 구성 요소

| 구분 | 무엇 | 어디에 | 근거 |
|---|---|---|---|
| AZ | 2개 (`2a`, `2c`) | | EKS 최소 2개 AZ |
| 퍼블릭 서브넷 | `10.0.1.0/24`, `10.0.2.0/24` | AZ당 1개 | ALB·NAT 가 놓이는 앞단 (`kubernetes.io/role/elb`) |
| 프라이빗 서브넷 | `10.0.11.0/24`, `10.0.12.0/24` | AZ당 1개 | 노드·파드, 공인 IP 없음 (`kubernetes.io/role/internal-elb`) |
| NAT | 1개 (`single_nat_gateway`) | 퍼블릭 한쪽 | 비용. 그 AZ 가 죽으면 양쪽 노드 다 바깥으로 못 나감 |
| ALB | 1개 = **유일한 입구** | 퍼블릭 두 AZ 에 걸침 | ALB 는 최소 2개 AZ 필요. Gateway API 로 요청 (`05-gateway.yaml`) — Gateway·LoadBalancerConfiguration(플랫폼) / HTTPRoute·TargetGroupConfiguration(앱) |
| 노드 | t3.medium × 2 (최대 2) | 프라이빗 | `terraform/eks.tf` |
| 앱 파드 | 예매 · Redis · Kafka 1대 (+토픽 생성 Job) | 노드 위 | `infra/eks/01~04` |
| EBS 볼륨 | Kafka 용 gp3 1Gi | Kafka 파드와 같은 AZ | `WaitForFirstConsumer` (`03-kafka-storage.yaml`) |
| 컨트롤러 | LB Controller · EBS CSI · CoreDNS · VPC CNI · kube-proxy | `kube-system` | 앞의 둘이 IRSA 로 AWS 호출. LB Controller 는 Gateway API CRD(v1.6.0 + 전용 CRD)가 먼저 있어야 Gateway 를 맡는다 |

## 트래픽 길

| 방향 | 경로 | 예 |
|---|---|---|
| 들어오는 길 | 인터넷 → IGW → ALB(퍼블릭) → 파드 IP(프라이빗) | 맥에서 `POST /reserve` |
| 나가는 길 | 노드(프라이빗) → NAT(퍼블릭) → IGW → AWS 서비스 | ECR 이미지 풀, STS 토큰 교환, EBS 생성 API |
| kubectl | 맥 → EKS 컨트롤플레인 퍼블릭 엔드포인트 (ALB 아님) | `endpoint_public_access = true` — 3단계에서 Private only 로 조인다 |

## 같이 볼 것
- **NAT 의 길** — 3단계에서 NAT 를 없애고 VPC 엔드포인트로 바꾸면 "나가는 길"이 통째로 바뀐다(ECR 풀 실패를 겪는 지점).
- **AZ 경계 함정 (수정 7)** — Kafka 가 `2a` 에 떠서 볼륨도 `2a` 에 생긴 뒤 파드가 `2c` 로 옮겨 가면 볼륨이 못 따라가 Pending. 4단계.

## 갱신 기록
- 2026-10-30 처음 작성 (LB 시리즈 ④ Ingress 까지)
- 2026-11-02 입구를 Gateway API 로 (#62). Ingress·NLB 는 지웠다 — 입구는 하나

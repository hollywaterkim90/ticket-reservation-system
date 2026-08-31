#!/usr/bin/env bash
# =====================================================================
# start-local.sh  (Mac / Linux)
#   minikube 기동부터 port-forward 까지 전체 스택을 한 번에 올린다.
#
#   사용법:
#     ./start-local.sh                 전체 실행 (최초 1회 또는 코드 변경 후)
#     ./start-local.sh --skip-build    이미지 빌드 생략 (코드 변경이 없을 때)
#     ./start-local.sh --help          도움말
#
#   종료:  ./stop-local.sh        (port-forward 만 정리)
#          kubectl delete -f infra/infra.yaml   (팟만 정리, 데이터는 볼륨에 남음)
#          minikube stop             (클러스터 정지)
#
#   각 단계는 멱등(idempotent)하다. 이미 완료된 단계는 건너뛴다.
#   (Windows 는 start-local.ps1 을 사용)
# =====================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
INFRA="$REPO/infra"
PIDFILE="$SCRIPT_DIR/.local-pf-pids"

MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-8192}"
SKIP_BUILD=0

for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=1 ;;
    --help|-h)    sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "알 수 없는 옵션: $arg (--help 참고)"; exit 1 ;;
  esac
done

fail() { echo "❌ $1"; exit 1; }

# ---------------------------------------------------------------------
# [0/6] 필수 도구 확인
# ---------------------------------------------------------------------
echo "==> [0/6] 필수 도구 확인"
for cmd in docker minikube kubectl helm; do
  command -v "$cmd" >/dev/null || fail "$cmd 가 설치되어 있지 않습니다. (brew install $cmd)"
done
docker info >/dev/null 2>&1 || fail "Docker 데몬이 응답하지 않습니다. Docker Desktop 을 실행하세요."
echo "    docker / minikube / kubectl / helm 확인"

# ---------------------------------------------------------------------
# [1/6] minikube 클러스터
# ---------------------------------------------------------------------
echo "==> [1/6] minikube 클러스터"
if minikube status 2>/dev/null | grep -q "host: Running"; then
  echo "    이미 실행 중 — 건너뜀"
else
  echo "    기동 (cpus=$MINIKUBE_CPUS, memory=${MINIKUBE_MEMORY}MB) — 수 분 소요"
  minikube start --driver=docker --cpus="$MINIKUBE_CPUS" --memory="$MINIKUBE_MEMORY" \
    || fail "minikube 기동 실패. Docker Desktop 의 메모리 할당(${MINIKUBE_MEMORY}MB 이상)을 확인하세요."
fi

# ---------------------------------------------------------------------
# [2/6] KEDA (Kafka lag 기반 오토스케일러)
# ---------------------------------------------------------------------
echo "==> [2/6] KEDA"
if kubectl get crd scaledobjects.keda.sh >/dev/null 2>&1; then
  echo "    이미 설치됨 — 건너뜀"
else
  echo "    helm 으로 설치"
  helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1
  helm repo update >/dev/null 2>&1
  helm install keda kedacore/keda --namespace keda --create-namespace >/dev/null \
    || fail "KEDA 설치 실패"
  kubectl wait --for=condition=ready pod --all -n keda --timeout=180s >/dev/null \
    || fail "KEDA 팟이 준비되지 않았습니다"
  echo "    설치 완료"
fi

# ---------------------------------------------------------------------
# [3/6] 서비스 이미지 빌드 후 minikube 로 로드
#   로컬 docker 로 만든 이미지는 minikube 내부에서 보이지 않으므로
#   image load 로 밀어넣어야 한다. (imagePullPolicy: IfNotPresent)
# ---------------------------------------------------------------------
echo "==> [3/6] 서비스 이미지"
build_and_load() {
  local dir="$1" image="$2"
  echo "    빌드: $image"
  (cd "$REPO/$dir" && docker build -q -t "$image" . >/dev/null) || fail "$image 빌드 실패"
  echo "    로드: $image -> minikube"
  minikube image load "$image" || fail "$image 로드 실패"
}

if [ "$SKIP_BUILD" -eq 1 ]; then
  echo "    --skip-build 지정 — 건너뜀"
else
  build_and_load "ticket-reservation-service" "ticket-reservation-service:latest"
  build_and_load "ticket-payment-service"     "ticket-payments-service:latest"
fi

# ---------------------------------------------------------------------
# [4/6] 인프라 배포 (Kafka x3 / Redis / ES / Kibana / Kafka-UI + 토픽)
# ---------------------------------------------------------------------
echo "==> [4/6] 인프라 배포 및 토픽 생성 대기"
# 완료된 Job 은 apply 로 재실행되지 않는다. 브로커에 볼륨이 없어 팟이 재시작되면
# 토픽이 사라지는데 Job 은 Complete 로 남아 있어 토픽이 복구되지 않으므로, 매번 지우고 다시 만든다.
# (Job 스크립트 자체에 브로커 대기 루프가 있어 순서는 문제되지 않는다)
kubectl delete job init-kafka-topics --ignore-not-found >/dev/null 2>&1
kubectl apply -f "$INFRA/volumes.yaml" >/dev/null || fail "볼륨 생성 실패"
kubectl apply -f "$INFRA/infra.yaml" >/dev/null || fail "인프라 배포 실패"
for d in kafka-1 kafka-2 kafka-3; do
  kubectl rollout status "deployment/$d" --timeout=180s >/dev/null || fail "$d 기동 실패"
done
kubectl wait --for=condition=complete job/init-kafka-topics --timeout=180s >/dev/null 2>&1 || true

# 토픽이 실제로 만들어졌는지 확인 — 없으면 이후 단계가 전부 무의미하다
topics="$(kubectl exec deploy/kafka-1 -- kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)"
for t in ticket-reservations ticket-payments ticket-reservations.DLQ; do
  echo "$topics" | grep -qx "$t" || fail "토픽 '$t' 생성 실패 — kubectl logs job/init-kafka-topics 확인"
done
echo "    Kafka 3 브로커 및 토픽 준비 완료"

# ---------------------------------------------------------------------
# [5/6] 애플리케이션 및 KEDA ScaledObject 배포
#   broker-alias: KEDA operator 는 keda 네임스페이스에서 동작하는데
#   Kafka 가 advertised.listener 로 짧은 이름을 반환하므로,
#   동일 이름의 ExternalName 서비스를 만들어 해석 문제를 우회한다.
# ---------------------------------------------------------------------
echo "==> [5/6] 애플리케이션 및 KEDA 배포"
kubectl apply -f "$INFRA/ticket-reservation-depl.yaml" >/dev/null
kubectl apply -f "$INFRA/ticket-payment-depl.yaml" >/dev/null
kubectl apply -f "$INFRA/ticket-payments-keda-broker-alias.yaml" >/dev/null
kubectl apply -f "$INFRA/ticket-payments-keda.yaml" >/dev/null

# 이미지를 새로 로드했다면 기존 팟이 옛 이미지를 물고 있으므로 재시작한다
if [ "$SKIP_BUILD" -eq 0 ]; then
  kubectl rollout restart deployment/ticket-reservation-service >/dev/null 2>&1 || true
  kubectl rollout restart deployment/ticket-payments-service    >/dev/null 2>&1 || true
fi

kubectl rollout status deployment/ticket-reservation-service --timeout=180s >/dev/null || fail "예매 서비스 기동 실패"
kubectl rollout status deployment/ticket-payments-service    --timeout=180s >/dev/null || fail "결제 서비스 기동 실패"
echo "    예매/결제 서비스 및 ScaledObject 배포 완료"

# ---------------------------------------------------------------------
# [6/6] port-forward
#   docker 드라이버에서는 NodePort 가 호스트 localhost 로 뚫리지 않으므로
#   port-forward 로 터널을 만든다. 쿠버네티스 리소스가 아니라
#   내 PC 에서 도는 클라이언트 프로세스라 YAML 로는 표현할 수 없다.
# ---------------------------------------------------------------------
start_pf() {
  local svc="$1" localp="$2" remotep="$3" holder
  holder="$(lsof -ti "tcp:$localp" 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
  if [ -n "$holder" ]; then
    echo "    포트 $localp 사용 중(PID $holder) 정리"
    kill -9 $holder 2>/dev/null || true
    sleep 1
  fi
  kubectl port-forward "svc/$svc" "${localp}:${remotep}" >/dev/null 2>&1 &
  echo $! >> "$PIDFILE"
  echo "    localhost:$localp -> svc/$svc (PID $!)"
}

echo "==> [6/6] port-forward"
: > "$PIDFILE"
start_pf "kafka-ui"                   8090 8090
start_pf "ticket-reservation-service" 8085 8085

cat <<EOF

완료.

  Kafka UI  : http://localhost:8090
  예매 API  : http://localhost:8085/reserve

  예매 요청 예시:
    curl -X POST http://localhost:8085/reserve \\
      -H "Content-Type: application/json" \\
      -d '{"userId":"user1","ticketId":"ticket:stock:god"}'

  KEDA 스케일 아웃 관찰:
    kubectl get hpa -w
    kubectl get pods -l app=ticket-payments-service -w

  정리:
    ./scripts/stop-local.sh      port-forward 종료
    kubectl delete -f infra/infra.yaml     팟 삭제 (데이터는 볼륨에 남음)
    kubectl delete -f infra/volumes.yaml   볼륨까지 삭제 (결제 기록·토픽 소실)
    minikube stop                클러스터 정지
EOF

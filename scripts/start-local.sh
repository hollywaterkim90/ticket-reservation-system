#!/usr/bin/env bash
# =====================================================================
# start-local.sh  (Mac / Linux)
#   전체 스택 배포 + 로컬 접속용 port-forward 자동 실행 (minikube)
#
#   사용법:  chmod +x start-local.sh && ./start-local.sh
#   종료:    ./stop-local.sh
#
#   참고: port-forward 는 쿠버네티스 리소스가 아니라 "내 PC에서 도는
#         클라이언트 프로세스"라 kubectl apply(YAML)로는 넣을 수 없습니다.
#         그래서 이 스크립트가 apply 이후 백그라운드로 실행해줍니다.
#   (Windows 는 start-local.ps1 을 사용하세요)
# =====================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"   # 저장소 루트 (scripts/ 의 상위)
INFRA="$REPO/infra"                    # 쿠버네티스 매니페스트 폴더
PIDFILE="$SCRIPT_DIR/.local-pf-pids"

echo "==> [1/4] 인프라 배포 (Kafka x3 / Redis / ES / Kibana / Kafka-UI + 토픽)"
kubectl apply -f "$INFRA/infra.yaml"

echo "==> [2/4] Kafka 브로커 및 토픽 생성 대기"
kubectl rollout status deployment/kafka-1 --timeout=180s
kubectl rollout status deployment/kafka-2 --timeout=180s
kubectl rollout status deployment/kafka-3 --timeout=180s
kubectl wait --for=condition=complete job/init-kafka-topics --timeout=180s || true

echo "==> [3/4] 애플리케이션 및 KEDA 배포"
kubectl apply -f "$INFRA/ticket-reservation-depl.yaml"
kubectl apply -f "$INFRA/ticket-payment-depl.yaml"
kubectl apply -f "$INFRA/ticket-payments-keda-broker-alias.yaml"   # keda 네임스페이스용 Kafka 별칭
kubectl apply -f "$INFRA/ticket-payments-keda.yaml"
kubectl rollout status deployment/ticket-reservation-service --timeout=180s
kubectl rollout status deployment/ticket-payments-service --timeout=180s

# ---------------------------------------------------------------------
# port-forward 헬퍼: 대상 포트가 이미 점유돼 있으면(좀비 등) 정리 후 백그라운드 실행
# ---------------------------------------------------------------------
start_pf() {
  local svc="$1" localp="$2" remotep="$3"
  local holder
  holder="$(lsof -ti "tcp:$localp" 2>/dev/null || true)"
  if [ -n "$holder" ]; then
    echo "   포트 $localp 사용 중(PID $holder) 정리"
    kill -9 $holder 2>/dev/null || true
    sleep 1
  fi
  kubectl port-forward "svc/$svc" "${localp}:${remotep}" >/dev/null 2>&1 &
  echo $! >> "$PIDFILE"
  echo "   port-forward: localhost:$localp -> svc/$svc (PID $!)"
}

echo "==> [4/4] port-forward 실행 (백그라운드)"
: > "$PIDFILE"   # PID 기록 초기화
start_pf "kafka-ui"                   8090 8090
start_pf "ticket-reservation-service" 8085 8085

echo ""
echo "완료. 아래 주소로 접속하세요."
echo "   Kafka UI  : http://localhost:8090"
echo "   예매 API  : http://localhost:8085/reserve"
echo ""
echo "터널을 내리려면:  ./stop-local.sh"

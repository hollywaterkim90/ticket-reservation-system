#!/usr/bin/env bash
# 스케일 아웃 중 컨슈머 수 / 팟 수 / lag 을 1초 간격으로 기록한다.
# 이슈 #12 · #16 의 측정 형식과 동일하게 출력하여 전후 비교가 가능하도록 한다.
#
#   사용:  ./scripts/watch-rebalance.sh            (Ctrl+C 로 종료)
#          ./scripts/watch-rebalance.sh | tee /tmp/rebalance.log
set -uo pipefail

GROUP="${GROUP:-ticket-group-payment-worker}"
APP="${APP:-ticket-payments-service}"

kcg() {
  kubectl exec deploy/kafka-1 -- kafka-consumer-groups \
    --bootstrap-server localhost:9092 --describe --group "$GROUP" "$@" 2>/dev/null
}

printf '%-10s %-12s %-9s %-14s %s\n' TIME CONSUMER PODS LAG STRATEGY
while true; do
  ts="$(date +%H:%M:%S)"

  # 컬럼: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
  desc="$(kcg)"

  # 파티션을 실제로 할당받은 고유 컨슈머 수
  consumer="$(echo "$desc" | awk '$3 ~ /^[0-9]+$/ && $7 != "-" {print $7}' \
              | sort -u | wc -l | tr -d ' ')"

  # LAG 합계 / 파티션 수  (커밋 전이면 LAG 이 "-" 이므로 0 으로 본다)
  lag_sum="$(echo "$desc" | awk '$6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}')"
  parts="$(echo "$desc" | awk '$3 ~ /^[0-9]+$/ {c++} END{print c+0}')"

  pods="$(kubectl get pods -l "app=$APP" --field-selector=status.phase=Running \
          --no-headers 2>/dev/null | wc -l | tr -d ' ')"

  strategy="$(kcg --state | awk '$1 ~ /^ticket-/ {print $(NF-2); exit}')"

  printf '%-10s consumer=%-4s pods=%-3s lag=%-8s %s\n' \
    "$ts" "$consumer" "$pods" "${lag_sum}/${parts}" "$strategy"
  sleep 1
done

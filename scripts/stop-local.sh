#!/usr/bin/env bash
# =====================================================================
# stop-local.sh  (Mac / Linux)
#   start-local.sh 가 띄운 로컬 port-forward 터널들을 정리한다.
#   (쿠버네티스 배포 리소스는 건드리지 않는다. 그건 kubectl delete 로.)
# =====================================================================
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$ROOT/.local-pf-pids"

# 1) PID 기록 파일 기준 종료
if [ -f "$PIDFILE" ]; then
  while read -r pid; do
    [ -n "$pid" ] || continue
    if kill "$pid" 2>/dev/null; then
      echo "port-forward 종료 (PID $pid)"
    fi
  done < "$PIDFILE"
  rm -f "$PIDFILE"
fi

# 2) 혹시 남아있는 포트 기준 백업 정리
for p in 8090 8085; do
  holder="$(lsof -ti "tcp:$p" 2>/dev/null || true)"
  if [ -n "$holder" ]; then
    echo "포트 $p 잔여 프로세스 종료 (PID $holder)"
    kill -9 $holder 2>/dev/null || true
  fi
done
echo "완료."

# =====================================================================
# start-local.ps1  (Windows / PowerShell)
#   전체 스택 배포 + 로컬 접속용 port-forward 자동 실행 (minikube)
#
#   사용법:  PowerShell 에서   .\start-local.ps1
#   종료:    .\stop-local.ps1
#
#   참고: port-forward 는 쿠버네티스 리소스가 아니라 "내 PC에서 도는
#         클라이언트 프로세스"라 kubectl apply(YAML)로는 넣을 수 없습니다.
#         그래서 이 스크립트가 apply 이후 백그라운드로 실행해줍니다.
#   (Mac/Linux 는 start-local.sh 를 사용하세요)
# =====================================================================
$scriptDir = $PSScriptRoot
$repo = Split-Path $scriptDir -Parent          # 저장소 루트 (scripts/ 의 상위)
$infra = Join-Path $repo 'infra'               # 쿠버네티스 매니페스트 폴더
$pidFile = Join-Path $scriptDir '.local-pf-pids'

Write-Host "==> [1/4] 인프라 배포 (Kafka x3 / Redis / ES / Kibana / Kafka-UI + 토픽)" -ForegroundColor Cyan
kubectl apply -f "$infra\volumes.yaml"
kubectl apply -f "$infra\infra.yaml"

Write-Host "==> [2/4] Kafka 브로커 및 토픽 생성 대기" -ForegroundColor Cyan
kubectl rollout status deployment/kafka-1 --timeout=180s
kubectl rollout status deployment/kafka-2 --timeout=180s
kubectl rollout status deployment/kafka-3 --timeout=180s
kubectl wait --for=condition=complete job/init-kafka-topics --timeout=180s

Write-Host "==> [3/4] 애플리케이션 및 KEDA 배포" -ForegroundColor Cyan
kubectl apply -f "$infra\ticket-reservation-depl.yaml"
kubectl apply -f "$infra\ticket-payment-depl.yaml"
kubectl apply -f "$infra\ticket-payments-keda-broker-alias.yaml"  # keda 네임스페이스용 Kafka 별칭
kubectl apply -f "$infra\ticket-payments-keda.yaml"
kubectl rollout status deployment/ticket-reservation-service --timeout=180s
kubectl rollout status deployment/ticket-payments-service --timeout=180s

# ---------------------------------------------------------------------
# port-forward 헬퍼: 대상 포트가 이미 점유돼 있으면(좀비 등) 정리 후 백그라운드 실행
# ---------------------------------------------------------------------
function Start-PortForward($svc, $local, $remote) {
    $line = netstat -ano | Select-String ":$local\s+.*LISTENING"
    if ($line) {
        $holderPid = ($line[0].Line.Trim() -split '\s+')[-1]
        Write-Host "   포트 $local 사용 중(PID $holderPid) 정리" -ForegroundColor Yellow
        taskkill /PID $holderPid /F | Out-Null
        Start-Sleep -Seconds 1
    }
    $proc = Start-Process kubectl -ArgumentList 'port-forward',"svc/$svc","${local}:${remote}" -WindowStyle Hidden -PassThru
    Add-Content -Path $pidFile -Value $proc.Id
    Write-Host "   port-forward: localhost:$local -> svc/$svc (PID $($proc.Id))" -ForegroundColor Green
}

Write-Host "==> [4/4] port-forward 실행 (백그라운드)" -ForegroundColor Cyan
Remove-Item $pidFile -ErrorAction SilentlyContinue   # PID 기록 초기화
Start-PortForward "kafka-ui"                   8090 8090
Start-PortForward "ticket-reservation-service" 8085 8085

Write-Host ""
Write-Host "완료. 아래 주소로 접속하세요." -ForegroundColor Green
Write-Host "   Kafka UI  : http://localhost:8090"
Write-Host "   예매 API  : http://localhost:8085/reserve"
Write-Host ""
Write-Host "터널을 내리려면:  .\stop-local.ps1"

# =====================================================================
# stop-local.ps1  (Windows / PowerShell)
#   start-local.ps1 이 띄운 로컬 port-forward 터널들을 정리한다.
#   (쿠버네티스 배포 리소스는 건드리지 않는다. 그건 kubectl delete 로.)
# =====================================================================
$root = $PSScriptRoot
$pidFile = Join-Path $root '.local-pf-pids'

# 1) PID 기록 파일 기준 종료
if (Test-Path $pidFile) {
    Get-Content $pidFile | ForEach-Object {
        $id = $_.Trim()
        if ($id) {
            try {
                Stop-Process -Id $id -Force -ErrorAction Stop
                Write-Host "port-forward 종료 (PID $id)" -ForegroundColor Yellow
            } catch { }
        }
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}

# 2) 혹시 남아있는 포트 기준 백업 정리
foreach ($p in 8090, 8085) {
    $line = netstat -ano | Select-String ":$p\s+.*LISTENING"
    if ($line) {
        $holderPid = ($line[0].Line.Trim() -split '\s+')[-1]
        Write-Host "포트 $p 잔여 프로세스 종료 (PID $holderPid)" -ForegroundColor Yellow
        taskkill /PID $holderPid /F | Out-Null
    }
}
Write-Host "완료." -ForegroundColor Green

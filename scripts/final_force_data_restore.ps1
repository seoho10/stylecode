# 로컬 터미널(CMD/PowerShell, Cursor 밖)에서 실행하세요.
# data/ JSON을 강제로 스테이징 후 커밋/푸시합니다. .env는 절대 포함되지 않습니다.

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "0. data JSON 파일 존재 확인..."
if (-not (Test-Path data/sales_daily.json)) { Write-Error "data/sales_daily.json 없음"; exit 1 }
if (-not (Test-Path data/brands.json)) { Write-Error "data/brands.json 없음"; exit 1 }
Write-Host "   OK"

Write-Host "1. 무시 규칙 확인 (data JSON이 왜 안 올라가는지)..."
$ignoreOut = git check-ignore -v data/sales_daily.json 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "   [원인] data/sales_daily.json 을 무시하는 규칙: $ignoreOut"
    Write-Host "   해결: 위에 나온 파일(예: 전역 .gitignore)에서 *.json 또는 data/*.json 규칙을 제거하거나,"
    Write-Host "   이 저장소 .gitignore 맨 아래에 !data/sales_daily.json !data/brands.json 이 이미 있는지 확인하세요."
    exit 1
}
Write-Host "   (이 저장소에서는 무시되지 않음)"

Write-Host "2. index.lock 제거..."
if (Test-Path .git/index.lock) { Remove-Item .git/index.lock -Force; Write-Host "   Removed." } else { Write-Host "   없음." }

Write-Host "3. data JSON 강제 추가..."
git add -f data/sales_daily.json data/brands.json

Write-Host "4. 스테이징된 파일 확인 (.env 포함 여부)..."
$staged = git diff --cached --name-only
if ($staged -match '\.env') {
    Write-Error ".env가 스테이징됨 - 중단. git reset HEAD .env 후 다시 실행하세요."
    exit 1
}
if (-not $staged -or ($staged -notmatch 'sales_daily|brands')) {
    Write-Host "   [실패] data JSON이 스테이징되지 않았습니다. 1번에서 나온 무시 규칙을 제거한 뒤 다시 실행하세요."
    exit 1
}
Write-Host $staged

Write-Host "5. 커밋 및 푸시..."
git commit -m "fix: final forced data restoration"
git push origin main

Write-Host "완료."

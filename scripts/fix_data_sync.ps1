# data/ JSON 파일을 Git에 다시 추적시키고 커밋/푸시하는 스크립트
# .env는 절대 포함되지 않음 (data/ 폴더만 추가)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

# 1. 인덱스 잠금 해제
if (Test-Path .git/index.lock) {
    Remove-Item .git/index.lock -Force
    Write-Host "Removed .git/index.lock"
}

# 2. data/ 캐시 제거
git rm --cached -r data/ 2>$null

# 3. data JSON 및 .gitkeep만 추가 (.env 제외)
git add -f data/sales_daily.json data/brands.json data/.gitkeep

# 4. .env가 스테이징에 포함되지 않았는지 확인
$staged = git diff --cached --name-only
if ($staged -match '\.env') {
    Write-Error ".env is staged - aborting. Unstage it: git reset HEAD .env"
    exit 1
}
Write-Host "Staged files:" ; git diff --cached --name-only

# 5. 커밋 및 푸시
git commit -m "fix: force sync data files"
git push origin main

Write-Host "Done."

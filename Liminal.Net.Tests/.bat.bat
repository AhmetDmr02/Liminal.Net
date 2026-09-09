@echo off
powershell -NoProfile -Command "Get-ChildItem -Recurse -Filter *.cs -File | Sort-Object FullName | ForEach-Object { \"===== $($_.FullName) =====\"; Get-Content -LiteralPath $_.FullName; \"\" } | Set-Content merged.txt"
pause
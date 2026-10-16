param(
    [int]$Iterations = 100,
    [int]$Parallel = 4,
    [string]$Filter = '',
    [int]$TimeoutSeconds = 1200,
    [switch]$NoBuild
)

$ErrorActionPreference = 'Continue'
$root = $PSScriptRoot
$work = Join-Path $env:TEMP 'liminal-coyote'
$coyote = (Get-Command coyote).Source

#These drive real sockets and timers that Coyote cant control, so its "potential deadlock" guesses are false alarms here
#proven deadlocks are still reported
$extraArgs = @{
    'Liminal.Net.Tests.Coyote.ConnectionHandshakePromotionGapCoyoteTests.Coyote_RapidRestart_CancelledConnectTask_NeverClobbersLocalClientId' = '--skip-potential-deadlocks'
    'Liminal.Net.Tests.Coyote.ConnectionHandshakePromotionGapCoyoteTests.Coyote_RapidRestart_OldReceiveLoop_NeverShutsDownNewSession' = '--skip-potential-deadlocks'
}

if (-not $NoBuild) {
    dotnet build (Join-Path $root 'Liminal.Net.Tests\Liminal.Net.Tests.csproj') -c Debug -v q
    if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
}

if (Test-Path $work) { Remove-Item $work -Recurse -Force }
New-Item -ItemType Directory $work | Out-Null
Copy-Item (Join-Path $root 'Liminal.Net.Tests\bin\Debug\net8.0\*') $work -Recurse

Push-Location $work
try {
    foreach ($dll in 'Liminal.Net.Tests.dll', 'Liminal.Net.dll') {
        & $coyote rewrite $dll *> $null
        if ($LASTEXITCODE -ne 0) { throw "coyote rewrite failed for $dll." }
    }

    #Asking for a test that doesnt exist makes Coyote list every test it found
    $tests = & $coyote test Liminal.Net.Tests.dll -m __list_coyote_tests__ -i 1 2>&1 |
        ForEach-Object { "$_".Trim() } |
        Where-Object { $_ -like 'Liminal.Net.Tests.*' -and $_ -like "*$Filter*" } |
        Sort-Object -Unique
}
finally {
    Pop-Location
}

if (-not $tests) { throw 'No Coyote tests found.' }
Write-Host "Running $($tests.Count) Coyote tests, $Iterations iterations each, $Parallel at a time."

$queue = [System.Collections.Generic.Queue[string]]::new([string[]]$tests)
$running = @()
$results = @()
$index = 0

while ($queue.Count -gt 0 -or $running.Count -gt 0) {
    while ($queue.Count -gt 0 -and $running.Count -lt $Parallel) {
        $test = $queue.Dequeue()
        $index++
        $out = Join-Path $work "out\$index"
        $log = Join-Path $work "out\$index.log"
        New-Item -ItemType Directory $out -Force | Out-Null
        $arguments = "test Liminal.Net.Tests.dll -m $test -i $Iterations --enable-memory-access-races -o `"$out`" $($extraArgs[$test])"
        $process = Start-Process $coyote -ArgumentList $arguments -WorkingDirectory $work -RedirectStandardOutput $log -WindowStyle Hidden -PassThru
        $running += [pscustomobject]@{ Test = $test; Process = $process; Log = $log; Out = $out; Started = Get-Date }
    }

    Start-Sleep -Milliseconds 500

    foreach ($run in @($running)) {
        $timedOut = ((Get-Date) - $run.Started).TotalSeconds -gt $TimeoutSeconds
        if (-not $run.Process.HasExited -and -not $timedOut) { continue }
        if ($timedOut -and -not $run.Process.HasExited) { $run.Process.Kill() }

        $text = Get-Content $run.Log -Raw -ErrorAction SilentlyContinue
        $found = [regex]::Matches("$text", 'Found (\d+) bugs?') | Select-Object -Last 1
        $errorLine = Get-ChildItem $run.Out -Filter *.txt -Recurse -ErrorAction SilentlyContinue |
            Select-String -Pattern 'coyote::error' -List | Select-Object -First 1
        $status = if ($timedOut) { 'TIMEOUT' } elseif ($found) { "$($found.Groups[1].Value) bugs" } else { 'NO RESULT' }

        $results += [pscustomobject]@{ Status = $status; Test = $run.Test; Error = if ($errorLine) { $errorLine.Line.Trim() } else { '' } }
        $mark = if ($status -eq '0 bugs') { 'ok ' } else { 'BAD' }
        Write-Host "[$mark] $status  $($run.Test)"
        $running = @($running | Where-Object { $_ -ne $run })
    }
}

$bad = @($results | Where-Object { $_.Status -ne '0 bugs' })
Write-Host ''
Write-Host "$($results.Count - $bad.Count)/$($results.Count) clean."
foreach ($b in $bad) {
    Write-Host "  $($b.Status)  $($b.Test)"
    if ($b.Error) { Write-Host "      $($b.Error)" }
}
Write-Host "Logs: $work\out"

if ($bad.Count -gt 0) { exit 1 }
exit 0

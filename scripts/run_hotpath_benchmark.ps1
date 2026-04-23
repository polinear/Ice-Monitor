param(
    [string]$RedisHost = "localhost",
    [int]$RedisPort = 6379,
    [string]$CameraCounts = "1,2",
    [double]$Fps = 1,
    [double]$WarmupSec = 6,
    [double]$MeasureSec = 15,
    [double]$DrainSec = 3,
    [int]$FrameSize = 320,
    [int]$MaxLen = 100,
    [string]$OutDir = "benchmarks/hotpath"
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
Set-Location $repoRoot

Write-Host "Starting benchmark worker services..."
docker compose -f docker-compose.yml -f docker-compose.benchmark.yml up -d --build worker_bench_1 worker_bench_2

Write-Host "Waiting for benchmark workers to become ready..."
for ($i = 0; $i -lt 60; $i++) {
    $ready = @(
        "worker_bench_1",
        "worker_bench_2"
    ) | ForEach-Object {
        $logs = docker compose -f docker-compose.yml -f docker-compose.benchmark.yml logs --no-log-prefix --tail 30 $_ 2>$null
        $logs -match "Worker ready"
    }

    if (($ready | Where-Object { $_ }).Count -eq 2) {
        break
    }

    Start-Sleep -Seconds 5
}

Write-Host "Running hot-path benchmark..."
python "$repoRoot\scripts\hotpath_benchmark.py" `
    --camera-counts $CameraCounts `
    --redis-host $RedisHost `
    --redis-port $RedisPort `
    --fps $Fps `
    --warmup-sec $WarmupSec `
    --measure-sec $MeasureSec `
    --drain-sec $DrainSec `
    --frame-size $FrameSize `
    --maxlen $MaxLen `
    --out-dir $OutDir

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$producerDir = Join-Path $root "rep02-produtor"
$leaderDir = Join-Path $root "rep02-lider"
$replicaDir = Join-Path $root "rep02-replica"
$checkDir = Join-Path $root "resultado-check"
$api = "http://127.0.0.1:5001/api/v0"

function Stop-MatchingProcess([string]$pattern) {
    $processes = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -match $pattern
    }

    foreach ($process in $processes) {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Compile-Project([string]$projectDir) {
    $outDir = Join-Path $projectDir "out"
    if (Test-Path $outDir) {
        Remove-Item -LiteralPath $outDir -Recurse -Force
    }
    New-Item -ItemType Directory -Path $outDir | Out-Null

    Push-Location $projectDir
    try {
        $sources = @(Get-ChildItem -LiteralPath (Join-Path $projectDir "src\rep02") -Filter "*.java" -File |
            Select-Object -ExpandProperty FullName)
        if ($sources.Count -eq 0) {
            throw "Nao foram encontrados ficheiros Java em $projectDir."
        }
        & javac -d $outDir $sources
        if ($LASTEXITCODE -ne 0) {
            throw "Falha a compilar $projectDir"
        }
    }
    finally {
        Pop-Location
    }
}

Write-Host "A parar processos anteriores da COMINDIR02..."
Stop-MatchingProcess "rep02\.(PartitionReplica|PartitionLeader|Producer)"
Stop-MatchingProcess "ipfs(\.exe)?\s+daemon"

Write-Host "A apagar historicos..."
Get-ChildItem -LiteralPath $leaderDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match "^lider-p\d+\.txt$" } |
    Remove-Item -Force
Get-ChildItem -LiteralPath $replicaDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match "^replica-[^-]+-p\d+\.txt$" } |
    Remove-Item -Force

if (Test-Path $checkDir) {
    Remove-Item -LiteralPath $checkDir -Recurse -Force
}
New-Item -ItemType Directory -Path $checkDir | Out-Null

Write-Host "A compilar os tres projetos..."
Compile-Project $producerDir
Compile-Project $leaderDir
Compile-Project $replicaDir

if (-not (Get-Command ipfs -ErrorAction SilentlyContinue)) {
    throw "O comando ipfs nao foi encontrado no PATH."
}

$pubsubEnabled = (& ipfs config Pubsub.Enabled 2>$null).Trim()
if ($pubsubEnabled -ne "true") {
    & ipfs config --json Pubsub.Enabled true
    if ($LASTEXITCODE -ne 0) {
        throw "Nao foi possivel ativar o PubSub do IPFS."
    }
}

Write-Host "A iniciar o daemon IPFS..."
Start-Process powershell.exe -WorkingDirectory $root -ArgumentList @(
    "-NoExit",
    "-Command",
    "ipfs daemon"
) | Out-Null

function Start-JavaProcess([string]$workingDir, [string]$classPath, [string[]]$arguments) {
    $argumentText = ($arguments -join " ")
    $command = "java -cp `"$classPath`" $argumentText"
    Start-Process powershell.exe -WorkingDirectory $workingDir -ArgumentList @(
        "-NoExit",
        "-Command",
        $command
    ) | Out-Null
}

Write-Host "A aguardar 15 segundos pelo arranque do daemon IPFS..."
Start-Sleep -Seconds 15

Write-Host "A iniciar as replicas..."
Start-JavaProcess $replicaDir ".\out" "rep02.PartitionReplica", "1", "0,1", $api
Start-JavaProcess $replicaDir ".\out" "rep02.PartitionReplica", "2", "1,2", $api
Start-JavaProcess $replicaDir ".\out" "rep02.PartitionReplica", "3", "2,0", $api

Start-Sleep -Seconds 2

Write-Host "A iniciar os lideres..."
Start-JavaProcess $leaderDir ".\out" "rep02.PartitionLeader", "0", "3", "127.0.0.1", $api
Start-JavaProcess $leaderDir ".\out" "rep02.PartitionLeader", "1", "3", "127.0.0.1", $api
Start-JavaProcess $leaderDir ".\out" "rep02.PartitionLeader", "2", "3", "127.0.0.1", $api

Start-Sleep -Seconds 2

Write-Host "A iniciar o produtor..."
Start-JavaProcess $producerDir ".\out" "rep02.Producer", "3", $api

Write-Host ""
Write-Host "COMINDIR02 iniciado."
Write-Host "No produtor, executa: rajada 3000 20"
Write-Host "Para verificar no fim, executa:"
Write-Host "  Copy-Item `"$leaderDir\lider-p*.txt`" `"$checkDir`" -Force"
Write-Host "  Copy-Item `"$replicaDir\replica-*-p*.txt`" `"$checkDir`" -Force"
Write-Host "  java -cp `"$replicaDir\out`" rep02.PartitionCheck 3 `"$checkDir`""

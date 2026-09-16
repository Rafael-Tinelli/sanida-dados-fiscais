param(
    [string]$OutputPath = "$PWD\c75-external-delivery-evidence.json"
)

$ErrorActionPreference = 'Stop'

$AuthorizedCommit = 'ccc5a31c3da7c1c93570df0337e553e5a06404ac'
$BasePublic = 'https://sanida.com.br/financas/calculadoras/assets'
$BaseRaw = "https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/$AuthorizedCommit/consumers/frontend"

$Assets = @(
    'folha-core.js',
    'salario-liquido.js',
    'folha-thirteenth.js',
    'decimo-terceiro.js',
    'folha-vacation.js',
    'ferias-clt.js',
    'folha-termination.js',
    'rescisao-clt.js'
)

function Get-Sha256Hex([byte[]]$Bytes) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

$client = [System.Net.Http.HttpClient]::new()
$client.DefaultRequestHeaders.UserAgent.ParseAdd('Sanida-C7.5-ExternalClient/1.0')
$client.DefaultRequestHeaders.CacheControl = [System.Net.Http.Headers.CacheControlHeaderValue]::new()
$client.DefaultRequestHeaders.CacheControl.NoCache = $true

$results = @()
$blocks = @()

try {
    foreach ($asset in $Assets) {
        $expectedUrl = "$BaseRaw/$asset"
        $publicUrl = "$BasePublic/$asset"

        $expectedResponse = $client.GetAsync($expectedUrl).GetAwaiter().GetResult()
        $expectedBody = $expectedResponse.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        $expectedStatus = [int]$expectedResponse.StatusCode
        $expectedSha = if ($expectedStatus -eq 200) { Get-Sha256Hex $expectedBody } else { $null }

        $publicResponse = $client.GetAsync($publicUrl).GetAwaiter().GetResult()
        $publicBody = $publicResponse.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        $publicStatus = [int]$publicResponse.StatusCode
        $publicSha = if ($publicStatus -eq 200) { Get-Sha256Hex $publicBody } else { $null }

        $match = ($expectedStatus -eq 200 -and $publicStatus -eq 200 -and $expectedSha -eq $publicSha)
        if (-not $match) {
            $blocks += "public_asset_mismatch:$asset"
        }

        $results += [ordered]@{
            asset = $asset
            expected_source_url = $expectedUrl
            expected_http_status = $expectedStatus
            expected_sha256 = $expectedSha
            public_url = $publicUrl
            public_http_status = $publicStatus
            public_sha256 = $publicSha
            match = $match
        }
    }
}
finally {
    $client.Dispose()
}

$status = if ($blocks.Count -eq 0) { 'PASS' } else { 'BLOCKED' }
$payload = [ordered]@{
    schema_version = '1.0.0'
    checkpoint = 'C7.5'
    mode = 'external_client_public_delivery_validation'
    status = $status
    production_mutated = $false
    authorized_commit = $AuthorizedCommit
    observed_at_utc = [DateTime]::UtcNow.ToString('o')
    assets_expected = 8
    assets_matching = @($results | Where-Object { $_.match }).Count
    assets = $results
    block_reasons = $blocks
}

$json = $payload | ConvertTo-Json -Depth 8
[IO.File]::WriteAllText($OutputPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))

$bytes = [IO.File]::ReadAllBytes($OutputPath)
$evidenceSha = Get-Sha256Hex $bytes

Write-Host "========== C7.5 EXTERNAL DELIVERY =========="
Write-Host "status=$status"
Write-Host "assets=$($payload.assets_matching)/8"
foreach ($item in $results) {
    Write-Host ("{0}=public:{1} expected:{2} match:{3}" -f $item.asset, $item.public_http_status, $item.expected_http_status, $item.match)
}
Write-Host "block_reasons=$($blocks | ConvertTo-Json -Compress)"
Write-Host "evidence=$OutputPath"
Write-Host "evidence_sha256=$evidenceSha"
Write-Host "production_mutated=False"
if ($status -eq 'PASS') {
    Write-Host 'C7.5_EXTERNAL_DELIVERY=PASS'
    exit 0
}
Write-Host 'C7.5_EXTERNAL_DELIVERY=BLOCKED'
exit 3

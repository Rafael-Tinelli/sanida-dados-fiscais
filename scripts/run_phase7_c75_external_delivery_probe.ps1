param(
    [string]$OutputPath = "$PWD\c75-external-delivery-evidence.json",
    [string]$AuthorizedCommit = '505144918d05fb4932064f6f9da9b9358c448ac0',
    [string]$ExpectedReleaseId = 'fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e',
    [string]$ExpectedCacheKey = '20260916-f06f10'
)

$ErrorActionPreference = 'Stop'

if ($AuthorizedCommit -notmatch '^[0-9a-fA-F]{40}$') {
    throw 'AuthorizedCommit must be a 40-character hexadecimal Git commit SHA.'
}
$AuthorizedCommit = $AuthorizedCommit.ToLowerInvariant()

try {
    Add-Type -AssemblyName System.Net.Http -ErrorAction Stop
}
catch {
    throw "System.Net.Http could not be loaded: $($_.Exception.Message)"
}

$Base = 'https://sanida.com.br'
$BasePublic = "$Base/financas/calculadoras/assets"
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
$Pages = [ordered]@{
    H25 = @{ path = '/financas/calculadoras/'; scripts = @(); cache_marker = '20260916-f06f10' }
    H26 = @{ path = '/financas/calculadoras/salario-liquido-clt/'; scripts = @('folha-core.js', 'salario-liquido.js'); cache_marker = '20260916-f06f10' }
    H27 = @{ path = '/financas/calculadoras/decimo-terceiro/'; scripts = @('folha-core.js', 'folha-thirteenth.js', 'decimo-terceiro.js'); cache_marker = '2.1.0' }
    H28 = @{ path = '/financas/calculadoras/ferias-clt/'; scripts = @('folha-core.js', 'folha-vacation.js', 'ferias-clt.js'); cache_marker = '20260917-h28-r1' }
    H29 = @{ path = '/financas/calculadoras/rescisao-clt/'; scripts = @('folha-core.js', 'folha-termination.js', 'rescisao-clt.js'); cache_marker = '20260917-h29-r1' }
}

function Get-Sha256Hex([byte[]]$Bytes) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Get-Http([System.Net.Http.HttpClient]$Client, [string]$Url) {
    $response = $Client.GetAsync($Url).GetAwaiter().GetResult()
    try {
        $body = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        return @{ status = [int]$response.StatusCode; body = $body }
    }
    finally {
        $response.Dispose()
    }
}

function Bytes-ToText([byte[]]$Bytes) {
    return [Text.Encoding]::UTF8.GetString($Bytes)
}

$client = New-Object System.Net.Http.HttpClient
$client.Timeout = [TimeSpan]::FromSeconds(30)
$client.DefaultRequestHeaders.UserAgent.ParseAdd('Sanida-C7.5-Operator-External-Client/2.1')
$client.DefaultRequestHeaders.CacheControl = New-Object System.Net.Http.Headers.CacheControlHeaderValue
$client.DefaultRequestHeaders.CacheControl.NoCache = $true

$assetResults = @()
$pageResults = [ordered]@{}
$fiscal = [ordered]@{}
$blocks = @()

try {
    foreach ($asset in $Assets) {
        $expected = Get-Http $client "$BaseRaw/$asset"
        # Exact URL used by the published pages. Do not append an artificial query string:
        # some origin/rewrite stacks can treat that as a different resource.
        $publicUrl = "$BasePublic/$asset"
        $public = Get-Http $client $publicUrl
        $expectedSha = if ($expected.status -eq 200) { Get-Sha256Hex $expected.body } else { $null }
        $publicSha = if ($public.status -eq 200) { Get-Sha256Hex $public.body } else { $null }
        $match = ($expected.status -eq 200 -and $public.status -eq 200 -and $expectedSha -eq $publicSha)
        if (-not $match) { $blocks += "public_asset_mismatch:$asset" }
        $assetResults += [ordered]@{
            asset = $asset
            expected_source_url = "$BaseRaw/$asset"
            expected_http_status = $expected.status
            expected_sha256 = $expectedSha
            public_url = $publicUrl
            public_http_status = $public.status
            public_sha256 = $publicSha
            match = $match
        }
    }

    foreach ($name in $Pages.Keys) {
        $spec = $Pages[$name]
        $r = Get-Http $client ($Base + $spec.path)
        $text = Bytes-ToText $r.body
        $pageBlocks = @()
        if ($r.status -ne 200) { $pageBlocks += "http:$($r.status)" }
        if ($text.Contains('Fatal error') -or $text.Contains('Parse error')) { $pageBlocks += 'php_error' }
        $positions = @()
        foreach ($script in $spec.scripts) { $positions += $text.IndexOf($script, [StringComparison]::Ordinal) }
        if ($positions.Count -gt 0) {
            $sorted = @($positions | Sort-Object)
            $badOrder = ($positions -contains -1) -or ((($positions -join ',') -ne ($sorted -join ','))) -or ((@($positions | Select-Object -Unique).Count) -ne $positions.Count)
            if ($badOrder) { $pageBlocks += 'script_order' }
        }

        $cacheMarker = [string]$spec.cache_marker
        $cacheMarkerCount = ([regex]::Matches($text, [regex]::Escape($cacheMarker))).Count
        if ($cacheMarkerCount -lt 1) { $pageBlocks += "cache_marker_missing:$cacheMarker" }
        if ($name -eq 'H26' -and $cacheMarkerCount -ne 2) { $pageBlocks += "h26_cache_key_count:$cacheMarkerCount" }

        if ($name -eq 'H25') {
            $required = @(
                '/financas/calculadoras/salario-liquido-clt/',
                '/financas/calculadoras/decimo-terceiro/',
                '/financas/calculadoras/ferias-clt/',
                '/financas/calculadoras/rescisao-clt/'
            )
            foreach ($link in $required) { if (-not $text.Contains($link)) { $pageBlocks += "required_link:$link" } }

            # Use ASCII-safe, unambiguous boundary tokens so Windows PowerShell 5.1
            # cannot turn an encoding mismatch into a false negative.
            $boundaryTokens = @('FGTS', '40%', 'aviso', 'seguro-desemprego', 'TRCT')
            foreach ($token in $boundaryTokens) {
                if (-not $text.Contains($token)) { $pageBlocks += "rescisao_boundary_token:$token" }
            }
        }

        foreach ($b in $pageBlocks) { $blocks += "$name`:$b" }
        $pageResults[$name] = [ordered]@{
            path = $spec.path
            http_status = $r.status
            bytes = $r.body.Length
            script_order = @($spec.scripts)
            script_positions = @($positions)
            cache_marker = $cacheMarker
            cache_marker_occurrences = $cacheMarkerCount
            block_reasons = @($pageBlocks)
        }
    }

    $healthR = Get-Http $client "$Base/blog/wp-json/sfa/v1/fiscal-health"
    $health = if ($healthR.status -eq 200) { (Bytes-ToText $healthR.body | ConvertFrom-Json) } else { $null }
    $healthPass = ($healthR.status -eq 200 -and $null -ne $health -and $health.status -eq 'healthy' -and $health.release_id -eq $ExpectedReleaseId)
    $fiscal.health = [ordered]@{ http_status = $healthR.status; status = $health.status; release_id = $health.release_id; pass = $healthPass }
    if (-not $healthPass) { $blocks += 'fiscal_health' }

    $releaseR = Get-Http $client "$Base/blog/wp-json/sfa/v1/fiscal-release"
    $release = if ($releaseR.status -eq 200) { (Bytes-ToText $releaseR.body | ConvertFrom-Json) } else { $null }
    $releasePass = ($releaseR.status -eq 200 -and $null -ne $release -and $release.release_id -eq $ExpectedReleaseId)
    $fiscal.release = [ordered]@{ http_status = $releaseR.status; release_id = $release.release_id; pass = $releasePass }
    if (-not $releasePass) { $blocks += 'fiscal_release' }

    $legacyR = Get-Http $client "$Base/blog/wp-json/sfa/v1/folha"
    $legacyPass = ($legacyR.status -eq 410)
    $fiscal.legacy_folha = [ordered]@{ http_status = $legacyR.status; pass = $legacyPass }
    if (-not $legacyPass) { $blocks += 'legacy_folha' }
}
finally {
    $client.Dispose()
}

$status = if ($blocks.Count -eq 0) { 'PASS' } else { 'BLOCKED' }
$payload = [ordered]@{
    schema_version = '1.1.0'
    checkpoint = 'C7.5'
    mode = 'external_client_public_delivery_validation'
    collector = 'operator_external_client'
    status = $status
    production_mutated = $false
    authorized_commit = $AuthorizedCommit
    expected_release_id = $ExpectedReleaseId
    expected_cache_key = $ExpectedCacheKey
    observed_at_utc = [DateTime]::UtcNow.ToString('o')
    assets_expected = $Assets.Count
    assets_matching = @($assetResults | Where-Object { $_.match }).Count
    assets = $assetResults
    pages = $pageResults
    fiscal = $fiscal
    block_reasons = @($blocks)
}

$json = $payload | ConvertTo-Json -Depth 12
[IO.File]::WriteAllText($OutputPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
$evidenceSha = Get-Sha256Hex ([IO.File]::ReadAllBytes($OutputPath))

Write-Host '========== C7.5 EXTERNAL DELIVERY =========='
Write-Host "authorized_commit=$AuthorizedCommit"
Write-Host "status=$status"
Write-Host "assets=$($payload.assets_matching)/$($payload.assets_expected)"
foreach ($item in $assetResults) { Write-Host ("{0}=public:{1} expected:{2} match:{3}" -f $item.asset, $item.public_http_status, $item.expected_http_status, $item.match) }
foreach ($name in $pageResults.Keys) { Write-Host ("{0}=http:{1} cache:{2}x{3} blocks:{4}" -f $name, $pageResults[$name].http_status, $pageResults[$name].cache_marker, $pageResults[$name].cache_marker_occurrences, (@($pageResults[$name].block_reasons) -join ',')) }
Write-Host "fiscal_health=$($fiscal.health.pass)"
Write-Host "fiscal_release=$($fiscal.release.pass)"
Write-Host "legacy_folha=$($fiscal.legacy_folha.pass)"
Write-Host "block_reasons=$($blocks | ConvertTo-Json -Compress)"
Write-Host "evidence=$OutputPath"
Write-Host "evidence_sha256=$evidenceSha"
Write-Host 'production_mutated=False'
if ($status -eq 'PASS') { Write-Host 'C7.5_EXTERNAL_DELIVERY=PASS'; exit 0 }
Write-Host 'C7.5_EXTERNAL_DELIVERY=BLOCKED'
exit 3

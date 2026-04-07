param(
  [string]$WorkspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$dataDir = Join-Path $WorkspaceRoot "data"
$mockPath = Join-Path $WorkspaceRoot "user apk\data\mock.ts"

function Normalize-Text([string]$Value) {
  if ([string]::IsNullOrWhiteSpace($Value)) {
    return ""
  }

  $normalized = $Value.ToLowerInvariant()
  $normalized = $normalized -replace '\.xlsx$', ''
  $normalized = $normalized -replace '\b(matka|panel|pannel|record|chart|charts|jodi)\b', ' '
  $normalized = $normalized -replace '\b(19|20)\d{2}\b', ' '
  $normalized = $normalized -replace '[^a-z0-9]+', ' '
  $normalized = $normalized -replace '\s+', ' '
  return $normalized.Trim()
}

function Get-CellText($Sheet, [int]$Row, [int]$Column) {
  return [string]($Sheet.Cells.Item($Row, $Column).Text)
}

function Normalize-Label([string]$Value) {
  return (($Value -replace '\s+', ' ').Trim())
}

function Format-Jodi([string]$Value) {
  $cleaned = (($Value -replace '\s+', '')).Trim()
  if ($cleaned -match '^\d+$') {
    return ([int]$cleaned).ToString("00")
  }
  if ($cleaned -eq "" -or $cleaned -eq "-" -or $cleaned -eq "--" -or $cleaned -eq "---") {
    return "--"
  }
  return $cleaned.PadLeft(2, "0").Substring([Math]::Max(0, $cleaned.Length - 2))
}

function Format-PannaDigit([string]$Value) {
  $cleaned = (($Value -replace '\s+', '')).Trim()
  if ($cleaned -match '^\d$') {
    return $cleaned
  }
  return $null
}

function Format-Panna([string[]]$Digits) {
  if ($Digits.Count -ne 3) {
    return "---"
  }
  if ($Digits -contains $null) {
    return "---"
  }
  return ($Digits -join "")
}

function Resolve-Slug([string]$BaseName, $Markets) {
  $needle = Normalize-Text $BaseName
  $best = $null
  $bestLength = -1

  foreach ($market in $Markets) {
    $name = $market.normalized
    if ([string]::IsNullOrWhiteSpace($name)) {
      continue
    }

    if ($needle.Contains($name) -or $name.Contains($needle)) {
      if ($name.Length -gt $bestLength) {
        $best = $market
        $bestLength = $name.Length
      }
    }
  }

  return $best
}

$mockRaw = Get-Content -LiteralPath $mockPath -Raw
$marketMatches = [regex]::Matches($mockRaw, '\{\s*slug:\s*"([^"]+)",\s*name:\s*"([^"]+)"')
$markets = foreach ($match in $marketMatches) {
  [pscustomobject]@{
    slug = $match.Groups[1].Value
    name = $match.Groups[2].Value
    normalized = Normalize-Text $match.Groups[2].Value
  }
}

$excel = $null
$generated = @()
$skipped = @()

try {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = $false
  $excel.DisplayAlerts = $false

  $files = Get-ChildItem -LiteralPath $dataDir -Filter *.xlsx | Sort-Object Name
  foreach ($file in $files) {
    $market = Resolve-Slug $file.BaseName $markets
    if (-not $market) {
      $skipped += [pscustomobject]@{ file = $file.Name; reason = "No market slug match" }
      continue
    }

    $workbook = $null
    try {
      $resolvedWorkbookPath = (Resolve-Path -LiteralPath $file.FullName).Path
      $workbook = $excel.Workbooks.Open($resolvedWorkbookPath)
      $sheet = $workbook.Worksheets.Item(1)
      $usedRange = $sheet.UsedRange
      $maxRows = [int]$usedRange.Rows.Count

      $jodiRows = New-Object System.Collections.Generic.List[object]
      $pannaRows = New-Object System.Collections.Generic.List[object]

      for ($row = 1; $row -le ($maxRows - 2); $row++) {
        $label = Normalize-Label (Get-CellText $sheet $row 1)
        if ([string]::IsNullOrWhiteSpace($label)) {
          continue
        }

        $hasData = $false
        for ($dayIndex = 0; $dayIndex -lt 7; $dayIndex++) {
          $baseCol = 2 + ($dayIndex * 3)
          if ((Get-CellText $sheet $row $baseCol) -or (Get-CellText $sheet $row ($baseCol + 1)) -or (Get-CellText $sheet $row ($baseCol + 2))) {
            $hasData = $true
            break
          }
        }
        if (-not $hasData) {
          continue
        }

        $jodiRow = New-Object System.Collections.Generic.List[string]
        $jodiRow.Add($label) | Out-Null
        $pannaRow = New-Object System.Collections.Generic.List[string]
        $pannaRow.Add($label) | Out-Null

        for ($dayIndex = 0; $dayIndex -lt 7; $dayIndex++) {
          $baseCol = 2 + ($dayIndex * 3)
          $open = Format-Panna @(
            (Format-PannaDigit (Get-CellText $sheet $row $baseCol)),
            (Format-PannaDigit (Get-CellText $sheet ($row + 1) $baseCol)),
            (Format-PannaDigit (Get-CellText $sheet ($row + 2) $baseCol))
          )
          $jodi = Format-Jodi (Get-CellText $sheet $row ($baseCol + 1))
          $close = Format-Panna @(
            (Format-PannaDigit (Get-CellText $sheet $row ($baseCol + 2))),
            (Format-PannaDigit (Get-CellText $sheet ($row + 1) ($baseCol + 2))),
            (Format-PannaDigit (Get-CellText $sheet ($row + 2) ($baseCol + 2)))
          )

          $jodiRow.Add($jodi) | Out-Null
          $pannaRow.Add($open) | Out-Null
          $pannaRow.Add($close) | Out-Null
        }

        $jodiRows.Add(@($jodiRow.ToArray())) | Out-Null
        $pannaRows.Add(@($pannaRow.ToArray())) | Out-Null
        $row += 2
      }

      if ($jodiRows.Count -eq 0 -or $pannaRows.Count -eq 0) {
        $skipped += [pscustomobject]@{ file = $file.Name; reason = "No chart rows parsed" }
        continue
      }

      $payload = [ordered]@{
        slug = $market.slug
        source = $file.Name
        jodi = @($jodiRows)
        panna = @($pannaRows)
      }

      $outputPath = Join-Path $dataDir ("{0}.chart.json" -f $market.slug)
      $payload | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $outputPath -Encoding UTF8

      $generated += [pscustomobject]@{
        slug = $market.slug
        source = $file.Name
        output = [System.IO.Path]::GetFileName($outputPath)
        jodiRows = $jodiRows.Count
        pannaRows = $pannaRows.Count
      }
    } catch {
      $skipped += [pscustomobject]@{
        file = $file.Name
        reason = $_.Exception.Message
      }
    } finally {
      if ($workbook) {
        $workbook.Close($false)
        [void][System.Runtime.Interopservices.Marshal]::ReleaseComObject($workbook)
      }
    }
  }
} finally {
  if ($excel) {
    $excel.Quit()
    [void][System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel)
  }
  [GC]::Collect()
  [GC]::WaitForPendingFinalizers()
}

[pscustomobject]@{
  generated = $generated
  skipped = $skipped
} | ConvertTo-Json -Depth 8

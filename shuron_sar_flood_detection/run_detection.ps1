$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
python (Join-Path $ScriptDir "sar_flood_detection.py") `
  --data-dir "D:\shuron\GT-data" `
  --output-dir "D:\shuron\flood_detection_sar\output" `
  --min-area-m2 100

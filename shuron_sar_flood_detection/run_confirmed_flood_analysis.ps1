$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
python (Join-Path $ScriptDir "analyze_confirmed_flood.py") `
  --data-dir "D:\shuron\GT-data" `
  --truth-path "D:\shuron\GT-data\sinsuiiki\shinsui.shp" `
  --output-dir "D:\shuron\confirmed_flood_sar_analysis\output" `
  --min-area-m2 100

# load_env.ps1
Get-Content .env | ForEach-Object {
    $name, $value = $_ -split '=', 2
    Set-Item -Path "env:$name" -Value $value
}
Write-Host "✅ Environment variables loaded."
# Search for Excel files that might be a channel mapping table
$searchPaths = @(
    "$env:USERPROFILE\Documents",
    "$env:USERPROFILE\Downloads",
    "$env:USERPROFILE\Desktop",
    "C:\Users\$env:USERNAME\OneDrive"
)

$keywords = @("channel", "mapping", "lookup", "reference", "codes", "IAM")

foreach ($path in $searchPaths) {
    if (Test-Path $path) {
        Get-ChildItem -Path $path -Recurse -Include "*.xlsx","*.xls" -ErrorAction SilentlyContinue |
        Where-Object { $name = $_.Name.ToLower(); $keywords | Where-Object { $name -like "*$_*" } } |
        Select-Object FullName, LastWriteTime, Length |
        Format-Table -AutoSize
    }
}

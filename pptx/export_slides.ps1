param(
    [string]$PptxPath = "C:\Users\andre\New_projects\VVD\pptx\VVD_v2_Report_TrackB.pptx",
    [string]$OutDir = "C:\Users\andre\New_projects\VVD\pptx\data\slides"
)

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$ppt = New-Object -ComObject PowerPoint.Application

$pres = $ppt.Presentations.Open($PptxPath, $true, $false, $false)

foreach ($slide in $pres.Slides) {
    $slidePath = Join-Path $OutDir ("slide_" + $slide.SlideIndex + ".png")
    $slide.Export($slidePath, "PNG", 1920, 1080)
    Write-Output "Exported slide $($slide.SlideIndex) -> $slidePath"
}

$pres.Close()
$ppt.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($ppt) | Out-Null
Write-Output "Done"

param
(
    [string]$FilesToZip = $(Throw "Missing parameter: -FilesToZip FilesToZip"),
    [string]$ZipOutputFilePath = $(Throw "Missing parameter: -ZipOutputFilePath ZipOutputFilePath"),
    [string]$Passw = $(Throw "Missing parameter: -Passw Passw")
)
$pathTo64Bit7Zip = "C:\Program Files\7-Zip\7z.exe";
$Params = "a -tzip ""$ZipOutputFilePath"" ""$FilesToZip"" -mx9 -p""$Passw"""
Start-Process $pathTo64Bit7Zip -ArgumentList $Params -Wait -PassThru -NoNewWindow

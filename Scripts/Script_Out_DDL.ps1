<#
.Synopsis
   Script out tables definitions
.DESCRIPTION
   Script out tables definitions based on SMO
.EXAMPLE
   .\Script_Out_DDL.ps1 '172.25.99.150\AMG_PROD' 'Enbrel_Temp_New' 'Ashfield_To_XSUNT' '\\172.25.99.150\f$\Temp'
.INPUTS
   None
.OUTPUTS
   No output
#>
param
(
    [string]$InstanceName = $(throw = "Missing parameter: -InstanceName Instancename"),
    [string]$DatabaseName = $(throw = "Missing parameter: -DatabaseName DatabaseName"),
    [string]$SchemaName = $(throw = "Missing parameter: -SchemaName SchemaName"),
    [string]$TableName = $(throw = "Missing parameter: -TableName TableName"),
    [string]$FilePath = $(throw = "Missing parameter: -FilePath FilePath")
)

set-psdebug -strict
# Load SMO assembly, and if we're running SQL 2008 DLLs load the SMOExtended and SQLWMIManagement libraries
$v = [System.Reflection.Assembly]::LoadWithPartialName( 'Microsoft.SqlServer.SMO')
if ((($v.FullName.Split(','))[1].Split('='))[1].Split('.')[0] -ne '9')
    {
        [System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMOExtended') | Out-Null
    }
# Handle any errors that occur
Trap
    {
        # Handle the error
        $err = $_.Exception
        Write-Host $err.Message
        while( $err.InnerException ) 
            {
                $err = $err.InnerException
                Write-Host $err.Message
            }
        # End the script.
        break
    }
# Create the root directory if it doesn't exist
#$guid = [GUID]::NewGuid()
$homedir = "$FilePath\"
if (!(Test-Path -path $homedir))
  {
    Try 
        {
            New-Item $homedir -type directory | out-null
        }
    Catch [system.exception]
        {
            Write-Error "Error while creating '$homedir'  $_"
            return
        }
  }
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($InstanceName);
$db = $server.Databases[$DatabaseName];
#$table = $db.Tables[$TableName];

$scripter = new-object Microsoft.SqlServer.Management.Smo.Scripter($server);
$scripter.options.ScriptBatchTerminator = $true
$scripter.options.FileName = $homedir + $TableName + '.sql'
$scripter.options.ToFileOnly = $true
$scripter.options.DriAll = $true
$scripter.options.Triggers = $true
$scripter.options.Indexes = $true

$dbObj = $db.enumobjects() | Where-Object { $_.schema -eq $SchemaName -and $_.name -eq $TableName }
$urn = new-object Microsoft.SqlServer.Management.Sdk.Sfc.Urn($dbObj.Urn);
$scripter.Script($urn);

#$scripter.Script([Microsoft.SqlServer.Management.Smo.SqlSmoObject[]]$table);

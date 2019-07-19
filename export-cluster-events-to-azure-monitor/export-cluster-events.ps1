$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = 'tls12'


############################
# Replace with your Log Analytics Workspace ID
$workspaceId = "00000000-0000-0000-0000-000000000000"
# Replace with your Log Analytics Primary Key
$sharedKey = "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000=="
# Specify the name of the record type that you'll be creating
$logType = "ClusterEvent"
# You can use an optional field to specify the timestamp from the data. If the time field is not specified, Azure Monitor assumes the time is the message ingestion time
$timeStampField = "timestamp"
# Your Databricks host URL
$DATABRICKS_HOST = "https://northeurope.azuredatabricks.net"
# Your Databricks Personal Access Token
$DATABRICKS_TOKEN = "dapi00000000000000000000000000000000" 
# Your Storage connection string 
$storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=STORAGEACCOUNT;AccountKey=00000000000000000000000000000000000000000000000000000000000000000000000000000000000000==;EndpointSuffix=core.windows.net"
############################

# Azure Table requires a partition key, but we don't need to partition our data, so we use a fixed value
$partitionKey = "dummy"

$ctx = New-AzStorageContext -ConnectionString $storageConnectionString

# Create the function to create the authorization signature
Function Build-Signature ($workspaceId, $sharedKey, $date, $contentLength, $method, $contentType, $resource)
{
    $xHeaders = "x-ms-date:" + $date
    $stringToHash = $method + "`n" + $contentLength + "`n" + $contentType + "`n" + $xHeaders + "`n" + $resource

    $bytesToHash = [Text.Encoding]::UTF8.GetBytes($stringToHash)
    $keyBytes = [Convert]::FromBase64String($sharedKey)

    $sha256 = New-Object System.Security.Cryptography.HMACSHA256
    $sha256.Key = $keyBytes
    $calculatedHash = $sha256.ComputeHash($bytesToHash)
    $encodedHash = [Convert]::ToBase64String($calculatedHash)
    $authorization = 'SharedKey {0}:{1}' -f $workspaceId,$encodedHash
    return $authorization
}


# Create the function to create and post the request
Function Post-LogAnalyticsData($workspaceId, $sharedKey, $body, $logType)
{
    $method = "POST"
    $contentType = "application/json"
    $resource = "/api/logs"
    $rfc1123date = [DateTime]::UtcNow.ToString("r")
    $contentLength = $body.Length
    $signature = Build-Signature `
        -workspaceId $workspaceId `
        -sharedKey $sharedKey `
        -date $rfc1123date `
        -contentLength $contentLength `
        -method $method `
        -contentType $contentType `
        -resource $resource
    $uri = "https://" + $workspaceId + ".ods.opinsights.azure.com" + $resource + "?api-version=2016-04-01"

    $headers = @{
        "Authorization" = $signature;
        "Log-Type" = $logType;
        "x-ms-date" = $rfc1123date;
        "time-generated-field" = $TimeStampField;
    }

    $response = Invoke-WebRequest -Uri $uri -Method $method -ContentType $contentType -Headers $headers -Body $body -UseBasicParsing
    return $response.StatusCode
}

$password = $DATABRICKS_TOKEN | ConvertTo-SecureString -asPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential("token",$password)

$watermark = Get-AzTableRow -Table $watermark_table -PartitionKey $partitionKey -RowKey "watermark"
if (!$watermark) {
    Add-AzTableRow -Table $watermark_table -PartitionKey $partitionKey -RowKey "watermark" -property @{"start_time"=0}
    $watermark = Get-AzTableRow -Table $watermark_table -PartitionKey $partitionKey -RowKey "watermark"
}

Write-Host "Starting at time watermark $($watermark.start_time)"

$end_time=[int64](([datetime]::UtcNow)-(get-date "1/1/1970")).TotalMilliseconds

$clusters = Invoke-RestMethod -Uri $DATABRICKS_HOST/api/2.0/clusters/list -Method GET -Headers @{Authorization = "Bearer $DATABRICKS_TOKEN"}

$clusters.clusters.ForEach({
    write-host $_.cluster_id

    $next_page = @{
        "cluster_id"=$_.cluster_id
        "order"="ASC"
        "start_time"=$watermark.start_time
        "end_time"=$end_time
        "limit"=100
    }

    while ($next_page) {
        $query = ConvertTo-Json $next_page -Depth 100

        $ret = Invoke-RestMethod -Uri $DATABRICKS_HOST/api/2.0/clusters/events -Method POST -Body $query -Headers @{Authorization = "Bearer $DATABRICKS_TOKEN"}
        $next_page = $ret.next_page

        Write-Host "Got $($ret.events.Count) events for cluster '$($_.cluster_id)'"

        $ret.events.ForEach({
            $eventId=$_.cluster_id + "/" + $_.timestamp + "/" + $_.type
            $rowKey=[uri]::EscapeDataString($eventId)
            if (Get-AzTableRow -Table $idrep_table -PartitionKey $partitionKey -RowKey $rowKey) {
                Write-Host "Ignoring already seen event"
                return
            }
            $json=ConvertTo-Json $_ -Depth 100
            Post-LogAnalyticsData -workspaceId $workspaceId -sharedKey $sharedKey -body ([System.Text.Encoding]::UTF8.GetBytes($json)) -logType $logType -TimeStampField $timeStampField
            Add-AzTableRow -Table $idrep_table -PartitionKey $partitionKey -RowKey $rowKey

        })

    }
})

Write-Host "Updating time watermark to $end_time"
$watermark.start_time = $end_time
Update-AzTableRow -Table $watermark_table -entity $watermark

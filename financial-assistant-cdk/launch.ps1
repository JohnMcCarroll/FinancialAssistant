# 1. Deploy the Infrastructure
Write-Host "--- Deploying CDK Stack ---" -ForegroundColor Cyan
cdk deploy --require-approval never

# 2. Extract the Glue Job Name from CDK (Assuming you named the output)
$jobName = "SEC-Ingestion-and-Embedding" # Or use: aws cloudformation describe-stacks ...

# 3. Wait for the EC2 instance to finish its setup (UserData)
# 2. The "Smart Ping" - Wait for ChromaDB to be alive
$outputs = aws cloudformation describe-stacks --stack-name FinancialAssistantCdkStack --query "Stacks[0].Outputs" | ConvertFrom-Json
$ip = ($outputs | Where-Object { $_.OutputKey -eq "ChromaPublicIP" }).OutputValue
$queryUrl = ($outputs | Where-Object { $_.OutputKey -eq "QueryUrl" }).OutputValue
Write-Host "--- Waiting for ChromaDB Heartbeat ---" -ForegroundColor Yellow
$heartbeatUrl = "http://$($ip):8000/api/v2/heartbeat"
$ready = $false
$attempts = 0

while (-not $ready -and $attempts -lt 30) {
    try {
        $response = Invoke-RestMethod -Uri $heartbeatUrl -Method Get -ErrorAction Stop
        if ($response) {
            $ready = $true
            Write-Host "`n[SUCCESS] ChromaDB is online!" -ForegroundColor Green
        }
    } catch {
        $attempts++
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 10
    }
}

# 4. Start the Glue Job
Write-Host "--- Starting Data Ingestion (Glue Job) ---" -ForegroundColor Cyan
$runId = aws glue start-job-run --job-name $jobName --query "JobRunId" --output text

Write-Host "Ingestion started. Run ID: $runId" -ForegroundColor Green
Write-Host "You can monitor logs at: https://console.aws.amazon.com/glue/home#jobRun:jobName=$jobName;runId=$runId"


Write-Host "Follow this link to chat with your AI financial advisor: $queryUrl."

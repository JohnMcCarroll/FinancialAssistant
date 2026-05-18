# Deploy CDK stack
Write-Host "Deploying CDK Stack" -ForegroundColor Cyan
cdk deploy --require-approval never

# Collect and parse CDK stack deployment outputs (names, urls, and UIDs of cloud assets)
$outputs = aws cloudformation describe-stacks --stack-name FinancialAssistantCdkStack --query "Stacks[0].Outputs" | ConvertFrom-Json
$OpenSearchEndpoint = ($outputs | Where-Object { $_.OutputKey -eq "OpenSearchEndpoint" }).OutputValue
$queryUrl = ($outputs | Where-Object { $_.OutputKey -eq "QueryUrl" }).OutputValue
$jobName = ($outputs | Where-Object { $_.OutputKey -eq "GlueJobName" }).OutputValue
$bucketName = ($outputs | Where-Object { $_.OutputKey -eq "ExportDataLakeName" }).OutputValue
$sqsurl = ($outputs | Where-Object { $_.OutputKey -eq "ExportSQSURL" }).OutputValue

# Initialize AWS SQS for SEC data ingestion
Write-Host "Collecting SEC Data" -ForegroundColor Yellow
python ./scripts/initialize_queue.py --SQSURL $sqsurl

# Initialize OpenSearch index (non-relational schema)
Write-Host "Initializing OpenSearch Index and Vector Mapping..." -ForegroundColor Cyan
python ./scripts/initialize_opensearch.py --endpoint $OpenSearchEndpoint --index "aapl_financials"

# Upload AWS Glue data processing script to S3 bucket
Write-Host "Uploading AWS Glue Data Chunking and Embedding Script to Data Lake" -ForegroundColor Yellow
aws s3 cp ./financial_assistant_cdk/streaming_pipeline.py "s3://$($bucketName)/scripts/streaming_pipeline.py"


# # Define SSM Parameter name
# $ssmParameterName = "/financial-datalake/sec-stream/status"

# # Reset the status parameter so we don't accidentally read stale data from a previous run
# Write-Host "Resetting stream status flag in SSM..." -ForegroundColor Yellow
# $status_flag = "INITIALIZING"
$glue_status = "STARTING"
# aws ssm put-parameter --name $ssmParameterName --value $status_flag --type "String" --overwrite | Out-Null

# # Initialize Datalake ingestion folder
# aws s3api put-object --bucket $bucketName --key "raw/"

# Launch AWS Glue job
Write-Host "Processing Data for Vector Database" -ForegroundColor Cyan
$runId = aws glue start-job-run --job-name $jobName --query "JobRunId" --output text
Write-Host "Ingestion started. Run ID: $runId" -ForegroundColor Green
Write-Host "You can monitor logs at: https://console.aws.amazon.com/glue/home#jobRun:jobName=$jobName;runId=$runId"

Write-Host "Waiting for Stream Glue Setup to Complete" -ForegroundColor Yellow

$attempts = 0
while ($status_flag -eq "INITIALIZING") {
    $status_flag = aws ssm get-parameter --name $ssmParameterName --query "Parameter.Value" --output text 2>$null
    $glue_status = aws glue get-job-run --job-name $jobName --run-id $runId --query "JobRun.JobRunState" --output text
    Write-Host "Current Status: $status_flag"
    if ($status_flag -eq "READY") {
        Write-Host "Glue stream is ready. Starting ingestion pipeline." -ForegroundColor Green
    } elseif ($glue_status -eq "FAILED" -or $glue_status -eq "STOPPED") {
        Write-Error "Glue Job failed. Check CloudWatch logs for $jobName"
        exit 1
    } else {
        $attempts++
        Start-Sleep -Seconds 10
    }
}

# Save Lambda function URL to file for front-end javascript retrieval
Write-Host "Connecting to frontend" -ForegroundColor Yellow
$envFilePath = "$PSScriptRoot/financial-frontend/.env.local"
$utf8 = New-Object System.Text.UTF8Encoding($false) # Ensure UTF-8 encoding
[System.IO.File]::WriteAllText($envFilePath, "VITE_QUERY_URL=$QueryUrl`n", $utf8)

# Launch website # TODO move to an ec2 instance in cloud
Write-Host "Serving Frontend Website" -ForegroundColor Green
Set-Location .\financial-frontend
npm run dev

# Deploy CDK stack
Write-Host "Deploying CDK Stack" -ForegroundColor Cyan
cdk deploy --require-approval never

# Collect and parse CDK stack deployment outputs (names, urls, and UIDs of cloud assets)
$outputs = aws cloudformation describe-stacks --stack-name FinancialAssistantCdkStack --query "Stacks[0].Outputs" | ConvertFrom-Json
$OpenSearchEndpoint = ($outputs | Where-Object { $_.OutputKey -eq "OpenSearchEndpoint" }).OutputValue
$queryUrl = ($outputs | Where-Object { $_.OutputKey -eq "QueryUrl" }).OutputValue
$jobName = ($outputs | Where-Object { $_.OutputKey -eq "GlueJobName" }).OutputValue
$bucketName = ($outputs | Where-Object { $_.OutputKey -eq "ExportDataLakeName" }).OutputValue
$ticker = "AAPL" #TODO: remove hardcoded ticker value

# # Download SEC data locally and upload to S3 Data Lake
# Write-Host "Running Local SEC Data Ingestion" -ForegroundColor Yellow
# python financial_assistant_cdk\ingest_sec_data.py --bucket_name $bucketName --ticker $ticker

# ### AWS GLUE - DATA PROCESSING (CHUNKING + EMBEDDING)
# # Upload AWS Glue data processing script to S3 bucket
# Write-Host "Uploading AWS Glue Data Chunking and Embedding Script to Data Lake" -ForegroundColor Yellow
# aws s3 cp ./financial_assistant_cdk/glue_ingestion.py "s3://$($bucketName)/scripts/glue_ingestion.py"

# # Launch AWS Glue job
# Write-Host "Processing Data for Vector Database" -ForegroundColor Cyan
# $runId = aws glue start-job-run --job-name $jobName --query "JobRunId" --output text
# Write-Host "Ingestion started. Run ID: $runId" -ForegroundColor Green
# Write-Host "You can monitor logs at: https://console.aws.amazon.com/glue/home#jobRun:jobName=$jobName;runId=$runId"

# Write-Host "Waiting for Ingestion to Complete" -ForegroundColor Yellow
# $status = "STARTING"
# $attempts2 = 0
# while ($status -eq "STARTING" -or $status -eq "RUNNING") {
#     $status = aws glue get-job-run --job-name $jobName --run-id $runId --query "JobRun.JobRunState" --output text
#     Write-Host "Current Status: $status"
#     if ($status -eq "SUCCEEDED") {
#         Write-Host "Data is ready! You can now use the Query URL." -ForegroundColor Green
#     } elseif ($status -eq "FAILED" -or $status -eq "STOPPED") {
#         Write-Error "Glue Job failed. Check CloudWatch logs for $jobName"
#         break
#     } else {
#         $attempts2++
#         Start-Sleep -Seconds 20
#     }
# }


### FRONT END WEBSITE
# Save Lambda function URL to file for front-end javascript retrieval
Write-Host "Connecting to frontend" -ForegroundColor Yellow
$envFilePath = "$PSScriptRoot/financial-frontend/.env.local"
$utf8 = New-Object System.Text.UTF8Encoding($false) # Ensure UTF-8 encoding
[System.IO.File]::WriteAllText($envFilePath, "VITE_QUERY_URL=$QueryUrl`n", $utf8)

# Launch website # TODO move to an ec2 instance in cloud
Write-Host "Serving Frontend Website" -ForegroundColor Green
Set-Location .\financial-frontend
npm run dev

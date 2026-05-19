# Deploy CDK stack
Write-Host "Deploying CDK Stack" -ForegroundColor Cyan
cdk deploy --require-approval never

# Collect and parse CDK stack deployment outputs (names, urls, and UIDs of cloud assets)
Write-Host "Collecting Asset Information" -ForegroundColor Cyan
$outputs = aws cloudformation describe-stacks --stack-name FinancialAssistantCdkStack --query "Stacks[0].Outputs" | ConvertFrom-Json
$OpenSearchEndpoint = ($outputs | Where-Object { $_.OutputKey -eq "OpenSearchEndpoint" }).OutputValue
$queryUrl = ($outputs | Where-Object { $_.OutputKey -eq "QueryUrl" }).OutputValue
$jobName = ($outputs | Where-Object { $_.OutputKey -eq "GlueJobName" }).OutputValue
$bucketName = ($outputs | Where-Object { $_.OutputKey -eq "DataLakeName" }).OutputValue
$sqsurl = ($outputs | Where-Object { $_.OutputKey -eq "IngestionSQSUrl" }).OutputValue

# Initialize OpenSearch index (non-relational schema)
Write-Host "Initializing OpenSearch Index and Vector Mapping" -ForegroundColor Cyan
python ./scripts/initialize_opensearch.py --endpoint $OpenSearchEndpoint --index "financial_docs"

# Upload AWS Glue data processing script to S3 bucket
Write-Host "Uploading AWS Glue Data Cleaning, Chunking, and Embedding Script to Data Lake" -ForegroundColor Cyan
aws s3 cp ./glue/clean_chunk_embed_glue.py "s3://$($bucketName)/scripts/clean_chunk_embed_glue.py"

# Launch AWS Glue job
Write-Host "Launching Data Processing Job" -ForegroundColor Cyan
$runId = aws glue start-job-run --job-name $jobName --query "JobRunId" --output text
Write-Host "Job started. Run ID: $runId" -ForegroundColor Green

# Initialize AWS SQS for SEC data ingestion
Write-Host "Initializing S&P500 SEC Data Ingestion Queue" -ForegroundColor Cyan
python ./scripts/initialize_queue.py --ignestion_sqs_url $sqsurl

# Save Lambda function URL to file for front-end javascript retrieval
Write-Host "Connecting to Frontend" -ForegroundColor Cyan
$envFilePath = "$PSScriptRoot/financial-frontend/.env.local"
$utf8 = New-Object System.Text.UTF8Encoding($false) # Ensure UTF-8 encoding
[System.IO.File]::WriteAllText($envFilePath, "VITE_QUERY_URL=$QueryUrl`n", $utf8)

# Launch website # TODO: move to an ec2 instance in cloud
Write-Host "Serving Frontend Website" -ForegroundColor Green
Set-Location .\financial-frontend
npm run dev

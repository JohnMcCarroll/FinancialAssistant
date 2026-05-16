import boto3
import json

# Initialize SQS client
sqs = boto3.client('sqs', region_name='us-east-1')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/sec-download-queue"

# Your list of target companies and years
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA"] 
years = ["2022", "2023", "2024", "2025"]

print(f"Queueing {len(tickers) * len(years)} download tasks...")

for ticker in tickers:
    for year in years:
        # Create a payload for each specific task
        payload = {
            "ticker": ticker,
            "year": year
        }
        
        # Send the task to SQS
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(payload)
        )
        print(f"Queued {ticker} for {year} - Message ID: {response['MessageId']}")

print("All tasks queued successfully!")
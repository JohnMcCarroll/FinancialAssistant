import boto3
import json
import argparse


def initialize_ingestion_sqs(queue_url):
    # Initialize SQS client
    sqs = boto3.client('sqs', region_name='us-east-1')
    # QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/sec-download-queue"

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
                QueueUrl=queue_url,
                MessageBody=json.dumps(payload)
            )
            print(f"Queued {ticker} for {year} - Message ID: {response['MessageId']}")

    print("All tasks queued successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--SQSURL", required=True)
    args = parser.parse_args()

    initialize_ingestion_sqs(args.SQSURL)
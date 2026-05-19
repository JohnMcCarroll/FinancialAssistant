import boto3
import json
import argparse
import pandas as pd
import requests
import io


def initialize_ingestion_sqs(queue_url):
    # Initialize SQS client
    sqs = boto3.client('sqs', region_name='us-east-1')

    # Programmatically fetch all ~500 stock tickers from Wikipedia
    print("Fetching S&P 500 tickers from Wikipedia...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        # Emulate a standard browser request header
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        }
        
        # Fetch the page content first
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Raise error if it's not a 200 OK
        
        # Pass the raw HTML string directly to pandas
        sp500_table = pd.read_html(io.StringIO(response.text))
        tickers = sp500_table[0]['Symbol'].tolist()
        
        # Clean formatting (Wikipedia uses '.' for share classes, e.g., BRK.B, but SEC/systems prefer '-')
        tickers = [ticker.replace('.', '-') for ticker in tickers]
    except Exception as e:
        print(f"Failed to fetch tickers programmatically: {e}")
        print("Falling back to baseline target list.")
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]

    # 30 years leading up to 2026 (1996 through 2025 inclusive)
    years = [str(year) for year in range(1996, 2026)]
    total_tasks = len(tickers) * len(years)
    
    print(f"Queueing {total_tasks} download tasks ({len(tickers)} tickers x {len(years)} years)...")

    batch_entries = []
    message_counter = 0

    for ticker in tickers:
        for year in years:
            payload = {
                "ticker": ticker,
                "year": year
            }
            
            # Formulate a safe, unique entry ID for the batch request
            entry_id = f"msg_{ticker}_{year}".replace('-', '_')
            batch_entries.append({
                'Id': entry_id,
                'MessageBody': json.dumps(payload)
            })
            
            # SQS allows a maximum of 10 messages per batch operation
            if len(batch_entries) == 10:
                sqs.send_message_batch(QueueUrl=queue_url, Entries=batch_entries)
                message_counter += 10
                if message_counter % 1000 == 0:
                    print(f"Progress: Queued {message_counter}/{total_tasks} tasks...")
                batch_entries = []

    # Clear out any remaining tasks left in the final partial batch
    if batch_entries:
        sqs.send_message_batch(QueueUrl=queue_url, Entries=batch_entries)
        message_counter += len(batch_entries)

    print(f"All {message_counter} tasks successfully written to SQS!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--SQSURL", required=True)
    args = parser.parse_args()

    initialize_ingestion_sqs(args.SQSURL)

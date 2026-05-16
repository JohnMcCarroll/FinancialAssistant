import boto3
from sec_edgar_downloader import Downloader
import os
import json

def handler(event, context):
    bucket_name = os.environ.get('BUCKET_NAME')
    email = "jtm5356@rit.edu" 
    s3 = boto3.client('s3')

    # SQS sends events in a "Records" list
    for record in event['Records']:
        # Parse the JSON payload we sent from the local script
        body = json.loads(record['body'])
        ticker = body['ticker']
        year = body['year']
        
        print(f"Processing task: {ticker} for {year}")

        # Construct date filters based on the year requested
        after_date = f"{year}-01-01"
        before_date = f"{year}-12-31"

        dl = Downloader("FinancialAssistant", email, "/tmp")
        
        try:
            # Download the 10-K for the specific time frame
            dl.get("10-K", ticker, after=after_date, before=before_date, download_details=False)

            base_path = f"/tmp/sec-edgar-filings/{ticker}/10-K"
            
            # Since we might download multiple folders, we iterate through them
            if os.path.exists(base_path):
                folders = os.listdir(base_path)
                for folder in folders:
                    file_path = os.path.join(base_path, folder, "full-submission.txt")
                    
                    if os.path.exists(file_path):
                        # Save to S3 using the ticker and the specific SEC filing folder name
                        s3_key = f"raw/{ticker}/{year}/{folder}-10-K.txt"
                        s3.upload_file(file_path, bucket_name, s3_key)
                        print(f"Success: Uploaded to s3://{bucket_name}/{s3_key}")
            else:
                print(f"No 10-K found for {ticker} in {year}.")

        except Exception as e:
            print(f"Error processing {ticker} for {year}: {str(e)}")
            # Raise the error so SQS knows the task failed and puts it back in the queue
            raise e

    return {"statusCode": 200, "body": "Batch processed successfully"}

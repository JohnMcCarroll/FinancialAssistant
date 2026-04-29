import boto3
from sec_edgar_downloader import Downloader
import os

# 1. Configuration
# Replace this with the bucket name from your CDK output!
BUCKET_NAME = "financialassistantcdkstac-financialdatalake00af577-knj1nezirfcr" 
TICKER = "AAPL"
EMAIL = "jtm5356@rit.edu" # SEC requires an email for their API

# 2. Download the 10-K from SEC
dl = Downloader("MyProject", EMAIL)
# This downloads the most recent 10-K for Apple
dl.get("10-K", TICKER, after="2023-01-01", download_details=False)

# 3. Find the downloaded file
# The downloader creates a nested folder structure
base_path = f"sec-edgar-filings/{TICKER}/10-K"
latest_folder = sorted(os.listdir(base_path))[-1]
file_path = os.path.join(base_path, latest_folder, "full-submission.txt")

# 4. Upload to your S3 Bucket
s3 = boto3.client('s3')
s3_key = f"raw/{TICKER}/10-K.txt"

print(f"Uploading {TICKER} 10-K to S3...")
s3.upload_file(file_path, BUCKET_NAME, s3_key)
print(f"Done! File is now at s3://{BUCKET_NAME}/{s3_key}")
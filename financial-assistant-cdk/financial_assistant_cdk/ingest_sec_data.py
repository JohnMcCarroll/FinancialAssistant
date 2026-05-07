import boto3
from sec_edgar_downloader import Downloader
import os
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket_name", required=True)
    parser.add_argument("--ticker", default="AAPL")
    args = parser.parse_args()

    # Collect args
    BUCKET_NAME = args.bucket_name
    TICKER = args.ticker
    EMAIL = "jtm5356@rit.edu" # SEC requires an email for their API

    print(f"Downloading {TICKER} data and uploading to {BUCKET_NAME}")

    # Download the 10-K from SEC database
    dl = Downloader("MyProject", EMAIL)
    # Download the most recent 10-K for Apple
    dl.get("10-K", TICKER, after="2023-01-01", download_details=False)

    base_path = f"sec-edgar-filings/{TICKER}/10-K"
    latest_folder = sorted(os.listdir(base_path))[-1]
    file_path = os.path.join(base_path, latest_folder, "full-submission.txt")

    # Upload to S3 Bucket
    s3 = boto3.client('s3')
    s3_key = f"raw/{TICKER}/10-K.txt"

    print(f"Uploading {TICKER} 10-K to S3")
    s3.upload_file(file_path, BUCKET_NAME, s3_key)
    print(f"File is now at s3://{BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":
    main()
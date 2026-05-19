import sys
import boto3
import json
import time
from urllib.parse import unquote_plus
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Extract runtime arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INGESTION_QUEUE_URL', 'OPENSEARCH_ENDPOINT'])

INDEX_NAME = "aapl_financials"
OPENSEARCH_ENDPOINT = args['OPENSEARCH_ENDPOINT']
QUEUE_URL = args['INGESTION_QUEUE_URL']

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =====================================================================
# WORKER NODE: PARALLEL PROCESSING LOGIC (Executed on cluster executors)
# =====================================================================
def process_partition(records):
    import boto3
    import json
    import re
    from bs4 import BeautifulSoup
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
    from requests_aws4auth import AWS4Auth

    session = boto3.Session()
    credentials = session.get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key, credentials.secret_key, 
        'us-east-1', 'es', session_token=credentials.token
    )

    openSearchClient = OpenSearch(
        hosts=[{'host': OPENSEARCH_ENDPOINT, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        max_retries=5,
        retry_on_timeout=True,
        timeout=30
    )
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

    def clean_sec_html(raw_sec_download):
        html_match = re.search(r'<html.*?>.*?</html>', raw_sec_download, re.DOTALL | re.IGNORECASE)
        if not html_match:
            return raw_sec_download
            
        html_content = html_match.group(0)
        soup = BeautifulSoup(html_content, 'lxml')
        
        ix_tags_to_drop = ['ix:header', 'ix:hidden', 'ix:resources', 'xbrli:context', 'xbrli:unit']
        for tag_name in ix_tags_to_drop:
            for element in soup.find_all(tag_name):
                element.decompose()
                
        for hidden_div in soup.find_all(style=True):
            if getattr(hidden_div, 'attrs', None) is not None:
                style_string = hidden_div.attrs.get('style').replace(" ", "").lower()
                if "display:none" in style_string:
                    hidden_div.decompose()
                
        for tag_name in ['script', 'style', 'noscript']:
            for element in soup.find_all(tag_name):
                element.decompose()

        clean_text = soup.get_text(separator="\n", strip=True)
        return re.sub(r'\n+', '\n', clean_text)

    def chunk_text(text, max_chars=3000, overlap=300):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=max_chars, 
            chunk_overlap=overlap,
            separators=["\n\n", "\n", ".", " ", ""]
        )
        return text_splitter.split_text(text)

    def get_embedding(text):
        body = json.dumps({"inputText": text})
        response = bedrock.invoke_model(
            body=body, 
            modelId="amazon.titan-embed-text-v2:0"
        )
        return json.loads(response['body'].read())['embedding']

    for row in records:
        file_path = row['path']
        binary_content = row['content']
        
        try:
            path_parts = file_path.split("/")
            raw_index = path_parts.index("raw")
            ticker = path_parts[raw_index + 1]
            year = path_parts[raw_index + 2]
        except ValueError:
            ticker, year = "UNKNOWN", "UNKNOWN"

        print(f"Worker processing file: {file_path} [{ticker} - {year}]")
        
        raw_html = binary_content.decode('utf-8', errors='ignore')
        clean_text = clean_sec_html(raw_html)

        # DEBUG
        print(clean_text)

        final_chunks = chunk_text(clean_text)
        
        bulk_actions = []
        for i, chunk in enumerate(final_chunks):
            try:
                vector = get_embedding(chunk)
                bulk_actions.append({
                    "_index": INDEX_NAME,
                    "_id": f"chunk_{ticker}_{year}_{i}",
                    "_source": {
                        "embedding": vector,
                        "text": chunk,
                        "metadata": {
                            "source": f"{ticker}_10K_{year}",
                            "ticker": ticker,
                            "year": year
                        }
                    }
                })
            except Exception as embed_err:
                print(f"Failed to generate embedding for chunk {i}: {str(embed_err)}")

        if bulk_actions:
            try:
                success, _ = helpers.bulk(openSearchClient, bulk_actions)
                print(f"Successfully indexed {success} chunks for {ticker}-{year}.")
            except Exception as bulk_err:
                print(f"Failed to commit bulk write to OpenSearch: {str(bulk_err)}")

    yield "Partition processing iteration completed."


# =====================================================================
# DRIVER NODE: CONTINUOUS ORCHESTRATION LOOP
# =====================================================================
sqs_client = boto3.client('sqs', region_name='us-east-1')

print("Starting continuous SQS event listener daemon...")

try:
    while True:
        s3_paths = []
        receipt_handles = []
        
        # Pull up to 50 messages per loop iteration to accumulate a mini-batch
        for _ in range(5):
            response = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5  # Long polling helps reduce API costs
            )
            
            messages = response.get('Messages', [])
            if not messages:
                break
                
            for msg in messages:
                try:
                    body = json.loads(msg['Body'])
                    records = body.get('Records', [])
                    for record in records:
                        bucket = record['s3']['bucket']['name']
                        key = unquote_plus(record['s3']['object']['key'])
                        s3_paths.append(f"s3://{bucket}/{key}")
                    
                    receipt_handles.append(msg['ReceiptHandle'])
                except Exception as parse_err:
                    print(f"Error parsing SQS payload: {str(parse_err)}")

        # If no new documents are waiting, sleep and poll again
        if not s3_paths:
            print("No new documents detected in queue. Sleeping for 20 seconds...")
            time.sleep(20)
            continue

        print(f"Discovered {len(s3_paths)} files. Spinning up Spark workers to process...")
        
        try:
            # Distribute the processing across the cluster infrastructure
            df = spark.read.format("binaryFile").load(s3_paths)
            processing_summary = df.rdd.mapPartitions(process_partition).collect()
            print(f"Batch processing run completed: {processing_summary}")
            
            # Delete messages from queue ONLY after cluster processing succeeds
            print("Clearing processed messages from SQS...")
            for handle in receipt_handles:
                sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=handle)
                
        except Exception as spark_err:
            print(f"CRITICAL: Spark processing batch failed: {str(spark_err)}")
            print("Messages will remain in SQS queue for visibility retry reset.")

except KeyboardInterrupt:
    print("Termination signal received. Exiting daemon execution safely.")

job.commit()

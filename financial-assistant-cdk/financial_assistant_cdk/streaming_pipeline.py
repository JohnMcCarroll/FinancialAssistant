import sys
import boto3
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, LongType, TimestampType


# Extract runtime arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CHECKPOINT_DIR', 'OPENSEARCH_ENDPOINT', 'BUCKET_NAME'])

# Define constants
INDEX_NAME = "aapl_financials" # TODO: rename
OPENSEARCH_ENDPOINT = args['OPENSEARCH_ENDPOINT']
BUCKET_NAME = args['BUCKET_NAME']

# Initialize Spark & Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Worker Node Partition Processing Logic
def process_partition(records):
    # Cluster-safe imports executed directly on worker environments
    import boto3
    import json
    import time
    import re
    from bs4 import BeautifulSoup
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
    from requests_aws4auth import AWS4Auth

    # Setup AWS Authentication context for OpenSearch inside worker thread
    session = boto3.Session()
    credentials = session.get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key, credentials.secret_key, 
        'us-east-1', 'es', session_token=credentials.token
    )

    # Instantiate isolated connection clients per cluster executor partition
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

    # Data Processing Helper Closures
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
            style_string = hidden_div['style'].replace(" ", "").lower()
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

    # Process files flowing through this specific partition cluster segment
    for row in records:
        file_path = row['path']
        binary_content = row['content']
        
        # Parse ticker and year out of the S3 key layout dynamically
        # Structure: s3://bucket-name/raw/<ticker>/<year>/...
        try:
            path_parts = file_path.split("/")
            raw_index = path_parts.index("raw")
            ticker = path_parts[raw_index + 1]
            year = path_parts[raw_index + 2]
        except ValueError:
            ticker, year = "UNKNOWN", "UNKNOWN"

        print(f"Worker processing whole document file: {file_path} [{ticker} - {year}]")
        
        # Convert binary data to string and execute extraction pipelines
        raw_html = binary_content.decode('utf-8', errors='ignore')
        clean_text = clean_sec_html(raw_html)
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
                print(f"Failed to generate embedding for chunk {i} on {file_path}: {str(embed_err)}")

        # Execute bulk payload indexing write directly from the worker to OpenSearch
        if bulk_actions:
            try:
                success, errors = helpers.bulk(openSearchClient, bulk_actions)
                print(f"Successfully indexed {success} chunks to OpenSearch for {ticker}-{year}.")
            except Exception as bulk_err:
                print(f"Failed to commit bulk index execution to OpenSearch: {str(bulk_err)}")

    yield f"Partition processing iteration completed."


# 3. Micro-Batch Orchestration Loop
def process_micro_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
        
    print(f"=== Triggering Micro-Batch: {batch_id} | Discovered New Files: {batch_df.count()} ===")
    
    # Ship data processing out across worker pool partitions using RDD mapPartitions transformation
    processing_summary = batch_df.rdd.mapPartitions(process_partition).collect()
    print(f"Micro-batch execution telemetry: {processing_summary}")


# 4. Bind Streaming Context Monitor Source

# Explicit schema signature for the Spark 'binaryFile' format
# This tells Spark exactly what the columns look like before any data arrives
binary_file_schema = StructType([
    StructField("path", StringType(), True),
    StructField("modificationTime", TimestampType(), True),
    StructField("length", LongType(), True),
    StructField("content", BinaryType(), True)
])

# Pointing directly to the /raw/ prefix. 
file_stream_source = spark.readStream \
    .format("binaryFile") \
    .schema(binary_file_schema) \
    .option("maxFilesPerTrigger", 1) \
    .load(f"s3://{BUCKET_NAME}/raw/")


# # Pointing directly to the /raw/ prefix. 'binaryFile' format recursively polls all sub-folders automatically.
# file_stream_source = spark.readStream \
#     .format("binaryFile") \
#     .option("maxFilesPerTrigger", 1) \
#     .load(f"s3://{BUCKET_NAME}/raw/")
# TODO: increase maxFilesPerTrigger if Bedrock API can handle it***

# Run continuous ingestion framework mapping execution states across engine checkpoints
query = file_stream_source.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", args['CHECKPOINT_DIR']) \
    .start()

time.sleep(10)

# 3. Signal to your local machine that the stream is open for business
ssm = boto3.client('ssm', region_name='us-east-1')
ssm.put_parameter(
    Name='/financial-datalake/sec-stream/status',
    Value='READY',
    Type='String',
    Overwrite=True
)

query.awaitTermination()

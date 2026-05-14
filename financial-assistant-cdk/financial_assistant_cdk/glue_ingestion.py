import sys
import boto3
from bs4 import BeautifulSoup
from awsglue.utils import getResolvedOptions
import json
from langchain_text_splitters import RecursiveCharacterTextSplitter
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
import time


# Get arguments passed from CDK
args = getResolvedOptions(sys.argv, ['OpenSearchEndpoint', 'BUCKET_NAME'])
openSearchEndpoint = args['OpenSearchEndpoint']
BUCKET_NAME = args['BUCKET_NAME']
INDEX_NAME = "aapl_financials"

print(f"OpenSeach Endpoint: {openSearchEndpoint}")
print(f"S3 Name: {BUCKET_NAME}")

# AWS authentication for OpenSearch
session = boto3.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 
                   'us-east-1', 'es', session_token=credentials.token)

# Initialize clients
s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
openSearchClient = OpenSearch(
    hosts=[{'host': openSearchEndpoint, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    max_retries=5,
    retry_on_timeout=True,
    timeout=30
)

def get_embedding(text):
    # Use Amazon Titan to generate semantic embedding vectors to index text chunks in vector database
    body = json.dumps({"inputText": text})
    response = bedrock.invoke_model(
        body=body, 
        modelId="amazon.titan-embed-text-v2:0"
    )
    # Extract the vector from Titan response
    response_body = response['body'].read()
    response_json = json.loads(response_body)
    vector = response_json['embedding']
    return vector

def chunk_text(text, max_chars=3000, overlap=300):
    # Splits text into manageable chunks
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=max_chars, 
        chunk_overlap=overlap,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    chunks = text_splitter.split_text(text)
    return chunks

# Prepare Bulk Actions
def generate_actions(chunks):
    for i, chunk in enumerate(chunks):
        vector = get_embedding(chunk)
        yield {
            "_index": INDEX_NAME,
            "_id": f"chunk_{i}",
            "_source": {
                "embedding": vector,
                "text": chunk,
                "metadata": {"source": "AAPL_10K_2023"}
            }
        }

# Read Raw Text from Data Lake
obj = s3.get_object(Bucket=BUCKET_NAME, Key="raw/AAPL/10-K.txt") # TODO: remove hardcoded s3 key
raw_html = obj['Body'].read().decode('utf-8')

# Old parsing
# # Remove HTML tags
# soup = BeautifulSoup(raw_html, "html.parser")
# clean_text = soup.get_text(separator=' ')

# New parsing attempt
soup = BeautifulSoup(raw_html, "lxml")
for tag in soup(["script", "style", "xbrl", "xml", "ix:nonnumeric", "ix:nonfraction"]):
    tag.decompose()
clean_text = soup.get_text(separator=' ', strip=True)

# Chunking Logic # TODO: implement hierarchical chunking
final_chunks = chunk_text(clean_text)
print(f"Original text length: {len(clean_text)}")
print(f"Created {len(final_chunks)} chunks.")

# Batched chunks upload to opensearch db

# openSearchClient.indices.put_settings(
#     index=INDEX_NAME,
#     body={"index": {"refresh_interval": "-1"}}
# )
batch_size = 50 
for i in range(0, len(final_chunks), batch_size):
    batch = final_chunks[i : i + batch_size]
    
    try:
        success, errors = helpers.bulk(openSearchClient, generate_actions(batch))
        print(f"Indexed batch {i//batch_size + 1}: {success} chunks.")
        time.sleep(1) 
        
    except Exception as e:
        print(f"Batch {i//batch_size + 1} failed: {e}")
        time.sleep(5)

# openSearchClient.indices.put_settings(
#     index=INDEX_NAME,
#     body={"index": {"refresh_interval": "1s"}}
# )

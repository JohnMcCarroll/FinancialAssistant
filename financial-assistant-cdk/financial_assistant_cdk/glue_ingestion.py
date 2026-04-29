import sys
import boto3
import chromadb
from chromadb.config import Settings
from bs4 import BeautifulSoup
from awsglue.utils import getResolvedOptions
import json

# Validation
import botocore
print(f"Boto3 version: {boto3.__version__}")
print(f"Botocore version: {botocore.__version__}")

# Get arguments passed from CDK
args = getResolvedOptions(sys.argv, ['CHROMA_IP', 'BUCKET_NAME'])
CHROMA_IP = args['CHROMA_IP']
BUCKET_NAME = args['BUCKET_NAME']

# Initialize Clients
s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
chroma_client = chromadb.HttpClient(
    host=CHROMA_IP, 
    port=8000,
    settings=Settings(allow_reset=True)
)
collection = chroma_client.get_or_create_collection(name="aapl_financials")

def get_embedding(text):
    # Use Amazon Titan for embeddings
    body = json.dumps({"inputText": text})
    response = bedrock.invoke_model(
        body=body, 
        modelId="amazon.titan-embed-text-v1"
    )
    
    # 1. Read the StreamingBody to get raw bytes
    response_body = response['body'].read()
    
    # 2. Convert bytes to a Python dictionary
    response_json = json.loads(response_body)
    
    # 3. Extract the vector
    return response_json['embedding']

def chunk_text(text, max_chars=20000, overlap=2000):
    """
    Splits text into manageable chunks. 
    If a chunk is too big, it splits by sentences, then by characters.
    """
    chunks = []
    start = 0
    while start < len(text):
        # Determine the end of the chunk
        end = start + max_chars
        chunk = text[start:end]
        chunks.append(chunk)
        # Move start point back by overlap to keep context between chunks
        start += (max_chars - overlap)
    return chunks

# --- Main Execution ---
# 1. Read Raw Text from S3
obj = s3.get_object(Bucket=BUCKET_NAME, Key="raw/AAPL/10-K.txt")
raw_html = obj['Body'].read().decode('utf-8')

# 2. Simple Cleaning (Removing HTML tags)
soup = BeautifulSoup(raw_html, "html.parser")
clean_text = soup.get_text(separator=' ')

# 3. Hierarchical Chunking Logic
final_chunks = chunk_text(clean_text)
print(f"Original text length: {len(clean_text)}")
print(f"Created {len(final_chunks)} chunks.")

for i, chunk in enumerate(final_chunks):
    try:
        vector = get_embedding(chunk)
        collection.add(
            ids=[f"chunk_{i}"],
            embeddings=[vector],
            metadatas=[{"source": "AAPL_10K_2023"}], 
            documents=[chunk]
        )
    except Exception as e:
        print(f"Failed on chunk {i}: {str(e)}")
        continue # Don't let one bad chunk kill the whole job

print(f"Successfully indexed {len(final_chunks)} chunks into ChromaDB.")

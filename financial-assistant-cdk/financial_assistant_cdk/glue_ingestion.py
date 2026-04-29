import sys
import boto3
import chromadb
from bs4 import BeautifulSoup
from awsglue.utils import getResolvedOptions

# Get arguments passed from CDK
args = getResolvedOptions(sys.argv, ['CHROMA_IP', 'BUCKET_NAME'])
CHROMA_IP = args['CHROMA_IP']
BUCKET_NAME = args['BUCKET_NAME']

# Initialize Clients
s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
chroma_client = chromadb.HttpClient(host=CHROMA_IP, port=8000)
collection = chroma_client.get_or_create_collection(name="aapl_financials")

def get_embedding(text):
    # Use Amazon Titan for embeddings
    body = {"inputText": text}
    response = bedrock.invoke_model(
        body=str(body).replace("'", '"'), 
        modelId="amazon.titan-embed-text-v1"
    )
    return response['body'].read().json()['embedding']

# 1. Read Raw Text from S3
obj = s3.get_object(Bucket=BUCKET_NAME, Key="raw/AAPL/10-K.txt")
raw_html = obj['Body'].read().decode('utf-8')

# 2. Simple Cleaning (Removing HTML tags)
soup = BeautifulSoup(raw_html, "html.parser")
clean_text = soup.get_text(separator=' ')

# 3. Hierarchical Chunking Logic
# We'll split by paragraphs (approximate)
paragraphs = [p.strip() for p in clean_text.split('\n\n') if len(p) > 200]

for i, p in enumerate(paragraphs):
    # 'Parent' is the full paragraph, 'Child' is just a snippet
    # For MVP, we'll index the paragraph with its own ID
    vector = get_embedding(p)
    collection.add(
        ids=[f"para_{i}"],
        embeddings=[vector],
        metadatas=[{"source": "AAPL_10K_2023", "text": p}], 
        documents=[p]
    )

print(f"Successfully indexed {len(paragraphs)} chunks into ChromaDB.")

import sys
import boto3
import chromadb
from bs4 import BeautifulSoup
from awsglue.utils import getResolvedOptions
import json
from langchain_text_splitters import RecursiveCharacterTextSplitter


# Get arguments passed from CDK
args = getResolvedOptions(sys.argv, ['CHROMA_IP', 'BUCKET_NAME'])
CHROMA_IP = args['CHROMA_IP']
BUCKET_NAME = args['BUCKET_NAME']

print(f"ChromaDB IP: {CHROMA_IP}")
print(f"S3 Name: {BUCKET_NAME}")

# Initialize clients
s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
chroma_client = chromadb.HttpClient(
    host=CHROMA_IP, 
    port=8000,
)
collection = chroma_client.get_or_create_collection(name="aapl_financials") #TODO: remove hardcoded collection name

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

# Read Raw Text from Data Lake
obj = s3.get_object(Bucket=BUCKET_NAME, Key="raw/AAPL/10-K.txt") # TODO: remove hardcoded s3 key
raw_html = obj['Body'].read().decode('utf-8')

# Remove HTML tags
soup = BeautifulSoup(raw_html, "html.parser")
clean_text = soup.get_text(separator=' ')

# Hierarchical Chunking Logic
final_chunks = chunk_text(clean_text)
print(f"Original text length: {len(clean_text)}")
print(f"Created {len(final_chunks)} chunks.")

# Batched chunks upload to ChromaDB
all_ids = []
all_embeddings = []
all_metadatas = []
all_documents = []

for i, chunk in enumerate(final_chunks):
    try:
        vector = get_embedding(chunk)
        all_ids.append(f"chunk_{i}")
        all_embeddings.append(vector)
        all_metadatas.append({"source": "AAPL_10K_2023"}) # TODO: remove hardcoded chunk sourse
        all_documents.append(chunk)
        
        # Every 100 chunks, push to DB
        if len(all_ids) >= 100:
            collection.add(
                ids=all_ids,
                embeddings=all_embeddings,
                metadatas=all_metadatas,
                documents=all_documents
            )
            print(f"Pushed batch up to chunk {i}")
            # Clear batches
            all_ids, all_embeddings, all_metadatas, all_documents = [], [], [], []
            
    except Exception as e:
        print(f"Error preparing chunk {i}: {str(e)}")

# Push any remaining chunks
if all_ids:
    collection.add(ids=all_ids, embeddings=all_embeddings, metadatas=all_metadatas, documents=all_documents)


# Debugging
print(f"VERIFICATION: Connection Host: {CHROMA_IP}")
print(f"VERIFICATION: Final count in collection: {collection.count()}")
print(f"VERIFICATION: Remote collections: {chroma_client.list_collections()}")
try:
    print(f"VERIFICATION: Client Settings: {chroma_client._settings}")
except:
    pass

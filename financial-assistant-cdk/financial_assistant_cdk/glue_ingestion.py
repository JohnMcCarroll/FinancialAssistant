import sys
import boto3
from bs4 import BeautifulSoup
from awsglue.utils import getResolvedOptions
import json
from langchain_text_splitters import RecursiveCharacterTextSplitter
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
import time
import re


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

# Ensure index exists
if not openSearchClient.indices.exists(index=INDEX_NAME):
    raise Exception("Target OpenSearch index does not exist.")
#     index_body = {
#         "settings": {
#             "index": {
#                 "knn": True
#             }
#         },
#         "mappings": {
#             "properties": {
#                 "embedding": {
#                     "type": "knn_vector",
#                     "dimension": 1024, # Dimension for Titan v2
#                     "method": {
#                         "name": "hnsw",
#                         "space_type": "l2",
#                         "engine": "nmslib"
#                     }
#                 },
#                 "text": {"type": "text"},
#                 "metadata": {"type": "object"}
#             }
#         }
#     }
#     openSearchClient.indices.create(index=INDEX_NAME, body=index_body)

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

# # New parsing attempt
# print(raw_html)
# doc_match = re.search(r'<html.*?>.*?</html>', raw_html, re.DOTALL | re.IGNORECASE)
# if doc_match:
#     clean_content = doc_match.group(0)
# else:
#     # Fallback: if the tag style is non-standard, scrub lines that look like uuencoded strings
#     print("Warning: Standard 10-K document tags not found. Applying aggressive line filter.")
#     lines = raw_html.split('\n')
#     # Uuencoded lines typically start with 'M', are exactly 61 chars long, and contain heavy punctuation
#     filtered_lines = [l for l in lines if not (len(l) >= 60 and l.startswith('M') and any(c in l for c in '$%@&!'))]
#     clean_content = '\n'.join(filtered_lines)
# soup = BeautifulSoup(raw_html, "lxml")
# for tag in soup(["script", "style", "xbrl", "xml", "ix:nonnumeric", "ix:nonfraction"]):
#     tag.decompose()
# clean_text = soup.get_text(separator=' ', strip=True)
# clean_text = re.sub(r'\s+', ' ', clean_text)

# new new parsing attempt
def clean_sec_html(raw_sec_download):
    print('RAW')
    print(raw_sec_download)

    soup1 = BeautifulSoup(raw_html, "html.parser")
    clean_text1 = soup1.get_text(separator=' ')
    print("HTML PARSE (OLD)")
    print(clean_text1)

    # 1. Isolate the true HTML document out of the SEC submission text file
    html_match = re.search(r'<html.*?>.*?</html>', raw_sec_download, re.DOTALL | re.IGNORECASE)
    if not html_match:
        print('NOT MATCH')
        # Fallback if the document is pre-iXBRL or raw text
        return raw_sec_download
        
    html_content = html_match.group(0)
    print("HTML_CONTENT")
    print(html_content)
    
    # 2. Parse with lxml
    soup = BeautifulSoup(html_content, 'lxml')
    
    # 3. Target and vaporize the iXBRL metadata headers
    # These tags hold the machine-readable "gibberish"
    ix_tags_to_drop = ['ix:header', 'ix:hidden', 'ix:resources', 'xbrli:context', 'xbrli:unit']
    for tag_name in ix_tags_to_drop:
        for element in soup.find_all(tag_name):
            element.decompose()
            
    # 4. Nuclear option for anything explicitly hidden via inline CSS
    for hidden_div in soup.find_all(style=True):
        style_string = hidden_div['style'].replace(" ", "").lower()
        if "display:none" in style_string:
            hidden_div.decompose()
            
    # 5. Clean up standard noisy tags you don't want embedded
    for tag_name in ['script', 'style', 'noscript']:
        for element in soup.find_all(tag_name):
            element.decompose()

    # 6. Extract clean text
    # Using strip=True and a newline separator keeps tables from running together
    clean_text = soup.get_text(separator="\n", strip=True)
    
    # Optional: Collapse egregious multi-newlines left behind by decomposed blocks
    clean_text = re.sub(r'\n+', '\n', clean_text)

    print("CLEAN")
    print(clean_text)
    
    return clean_text


clean_text = clean_sec_html(raw_html)

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

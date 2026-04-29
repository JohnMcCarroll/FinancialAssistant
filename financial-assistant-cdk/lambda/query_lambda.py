import json
import os
import boto3
import urllib3

bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
http = urllib3.PoolManager()

CHROMA_IP = os.environ['CHROMA_IP']
CHROMA_URL = f"http://{CHROMA_IP}:8000/api/v1"

def get_embedding(text):
    body = json.dumps({"inputText": text})
    response = bedrock.invoke_model(
        body=body, 
        modelId="amazon.titan-embed-text-v1"
    )
    return json.loads(response['body'].read())['embedding']

def handler(event, context):
    # 1. Parse the user's question
    user_query = "What are Apple's supply chain risks?"
    if event.get('queryStringParameters'):
        user_query = event['queryStringParameters'].get('q', user_query)

    # 2. Vectorize the question
    query_vector = get_embedding(user_query)

    # 3. Query ChromaDB (via API)
    # We're looking for the top 3 relevant paragraphs
    search_payload = {
        "query_embeddings": [query_vector],
        "n_results": 3
    }
    
    # Note: In a real app, you'd use the chromadb-client library
    # Here we hit the raw endpoint for simplicity
    res = http.request(
        'POST', 
        f"{CHROMA_URL}/collections/YOUR_COLLECTION_ID/query", # You'll need the ID from the Glue job output
        body=json.dumps(search_payload)
    )
    results = json.loads(res.data)
    
    # 4. Construct the Prompt for Claude
    context_text = "\n".join(results['documents'][0])
    prompt = f"""
    Human: Use the following excerpts from Apple's 10-K to answer the question.
    Context: {context_text}
    
    Question: {user_query}
    
    Assistant: Based on the 10-K filing,
    """

    # 5. Call Claude 3.5 Sonnet
    llm_response = bedrock.invoke_model(
        modelId="anthropic.claude-3-sonnet-20240229-v1:0",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}]
        })
    )
    
    answer = json.loads(llm_response['body'].read())['content'][0]['text']

    return {
        "statusCode": 200,
        "body": json.dumps({"answer": answer})
    }


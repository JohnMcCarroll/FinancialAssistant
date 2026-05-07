import json
import os
import boto3
import urllib3
import logging


# Define constants by collecting AWS asset info from the environment
CHROMA_IP = os.environ.get('CHROMA_IP')
COLLECTION_NAME = os.environ.get('COLLECTION_NAME', 'aapl_financials') # TODO: extend to multiple collections
CHROMA_URL = f"http://{CHROMA_IP}:8000/api/v2/tenants/default_tenant/databases/default_database"

# Setup Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Setup LLM and HTTP clients
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
http = urllib3.PoolManager()

def get_collection_id():
    # Fetches the UID for processed data's collection name
    try:
        response = http.request('GET', f"{CHROMA_URL}/collections")
        collections = json.loads(response.data)

        # Log processed data in ChromaDB
        found_names = [c['name'] for c in collections]
        logger.info(f"Collections in DB: {found_names}")

        for col in collections:
            if col['name'] == COLLECTION_NAME:
                return col['id']

        raise Exception(f"Collection '{COLLECTION_NAME}' not found. \nCollections found: {str(found_names)}")
    except Exception as e:
        raise Exception(f"Failed to fetch collection ID: {str(e)}")

def get_embedding(text):
    # Pass chunk of source text to embedding model and return semantic vector
    body = json.dumps({"inputText": text})
    response = bedrock.invoke_model(
        body=body, 
        modelId="amazon.titan-embed-text-v2:0"
    )
    return json.loads(response['body'].read())['embedding']

def handler(event, context):
    # Called when Lambda function invoked, this function is responsible for coordinating the interaction
    # between user and LLM Financial Assistant. The user's query is embedded, the vector database is searched
    # for relevant text data, said data is injected into a prompt with the user's question, the prompt is
    # sent to the LLM, and the LLM's answer is packaged into an HTTP response.
    try:
        # Fetch collection ID
        collection_id = get_collection_id()
        logger.info(f"Resolved Collection ID for '{COLLECTION_NAME}': {collection_id}")

        # Parse the user's question
        user_query = "What are Apple's supply chain risks?"
        if event.get('queryStringParameters'):
            user_query = event['queryStringParameters'].get('q', user_query)

        # Embed the question into a vector
        query_vector = get_embedding(user_query)
        logger.info(f"Query Vector (first 5 dims): {query_vector[:5]}")

        # Query ChromaDB via API TODO: use chromadb-client
        search_payload = {
            "query_embeddings": [query_vector],
            "n_results": 5
        }
        res = http.request(
            'POST', 
            f"{CHROMA_URL}/collections/{collection_id}/query", 
            body=json.dumps(search_payload),
            headers={'Content-Type': 'application/json'}
        )

        # DEBUGGING ChromaDB connection
        if res.status != 200:
            logger.info(
                f"Error connecting to ChromaDB API.\nStatus: {res.status}\n{res.data.decode('utf-8')}"
            )

        results = json.loads(res.data)
        
        # Construct the LLM prompt
        context_text = "\n\n".join(results['documents'][0])
        prompt = f"""
        You are a senior financial analyst.
        Below are contextually relevant excerpts from company 10-K filings and earnings call transcripts.
        If appropriate, use the provided context to answer the question below.

        Context: {context_text}
        
        Question: {user_query}
        """

        logger.info(f"FINAL RAG PROMPT SENT TO CLAUDE:\n{prompt}")

        # Send prompt to LLM
        model_id = "us.amazon.nova-lite-v1:0"
        native_request = {
            "inferenceConfig": {
                "maxTokens": 1000,
                "temperature": 0.7,
                "topP": 0.9,
            },
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"text": prompt}
                    ]
                }
            ]
        }

        llm_response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps(native_request)
        )

        model_response = json.loads(llm_response["body"].read())
        logger.info(f"LLM RESPONSE:\n{model_response}")
        
        answer = model_response['output']['message']['content'][0]['text']

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json", 
                "Access-Control-Allow-Origin": "*", # Allow the browser to read the response
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
            },
            "body": json.dumps({
                "answer": answer,
                "source_chunks_found": len(results['documents'][0])
            })
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

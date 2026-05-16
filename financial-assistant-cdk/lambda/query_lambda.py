import json
import os
import boto3
import logging
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


# Define constants
OpenSearchEndpoint = os.environ.get('OpenSearchEndpoint')
INDEX_NAME = os.environ.get('COLLECTION_NAME', 'aapl_financials') # TODO: extend to multiple collections

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Setup LLM client
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

def get_opensearch_client(endpoint):
    service = 'es'
    region = 'us-east-1'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 
                       region, service, session_token=credentials.token)
    client = OpenSearch(
        hosts=[{'host': endpoint, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        max_retries=5,
        retry_on_timeout=True,
        timeout=30
    )
    return client

def get_embedding(text):
    # Pass chunk of source text to embedding model and return semantic vector
    body = json.dumps({"inputText": text})
    response = bedrock.invoke_model(
        body=body, 
        modelId="amazon.titan-embed-text-v2:0"
    )
    embedding = json.loads(response['body'].read())['embedding']
    return embedding

def handler(event, context):
    # Called when Lambda function invoked, this function is responsible for coordinating the interaction
    # between user and LLM Financial Assistant. The user's query is embedded, the vector database is searched
    # for relevant text data, said data is injected into a prompt with the user's question, the prompt is
    # sent to the LLM, and the LLM's answer is packaged into an HTTP response.
    try:
        client = get_opensearch_client(OpenSearchEndpoint)


        # debugging: check entries in opensearch
        res = client.search(index=INDEX_NAME, body={"size": 3, "query": {"match_all": {}}})
        for hit in res['hits']['hits']:
            print(f"ID: {hit['_id']}")
            print(f"Text Preview: {hit['_source']['text'][:500]}...") 
            print("-" * 50)

        # Parse the user's question
        user_query = "What are Apple's supply chain risks?"
        if event.get('queryStringParameters'):
            user_query = event['queryStringParameters'].get('q', user_query)

        # Embed the question into a vector
        query_vector = get_embedding(user_query)
        logger.info(f"Query Vector (first 5 dims): {query_vector[:5]}")

        # k-NN index search in vector database
        search_query = {
            "size": 5,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_vector,
                        "k": 5
                    }
                }
            }
        }
        response = client.search(
            body=search_query,
            index=INDEX_NAME
        )
        hits = response['hits']['hits']
        retrieved_chunks = [hit['_source']['text'] for hit in hits]
        context_text = "\n\n".join(retrieved_chunks)
        
        # Construct the LLM prompt
        prompt = f"""
        You are a senior financial analyst.
        Below are contextually relevant excerpts from company 10-K filings and earnings call transcripts.
        If appropriate, use the provided context to answer the question below.

        Context: {context_text}
        
        Question: {user_query}
        """

        logger.info(f"FINAL RAG PROMPT SENT TO LLM:\n{prompt}")

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
                # "Access-Control-Allow-Origin": "*", # Bypass browser CORS?
                # "Access-Control-Allow-Headers": "Content-Type",
                # "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
            },
            "body": json.dumps({
                "answer": answer,
                "source_chunks_found": len(retrieved_chunks)
            })
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json", 
                # "Access-Control-Allow-Origin": "*", # Bypass browser CORS?
                # "Access-Control-Allow-Headers": "Content-Type",
                # "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
            },
            "body": json.dumps({"error": str(e)})
        }

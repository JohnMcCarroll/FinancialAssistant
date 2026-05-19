import argparse
import sys
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


def initialize_index(endpoint, index_name):
    region = 'us-east-1'
    service = 'es'
    
    # Setup AWS Authentication
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region, service)

    client = OpenSearch(
        hosts=[{'host': endpoint.replace('https://', ''), 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Define index settings + mapping
    index_body = {
        "settings": {
            "index": {
                "knn": True, # enable KNN search
                "knn.algo_param.ef_search": 100,
                "refresh_interval": "30s"   # slower refresh intervale to increase upload speed
            }
        },
        "mappings": {
            "properties": {
                "embedding": {
                    "type": "knn_vector",
                    "dimension": 1024,      # size of output vector from AWS Titan embedding model
                    "method": {
                        "name": "hnsw",     # Hierarchical Navigable Small World alg - TODO: implement hierarchical chunking
                        "space_type": "innerproduct",   # fast vector-space comparison alg
                        "engine": "faiss",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 16
                        }
                    }
                },
                "text": {"type": "text"},
                "metadata": {
                    "properties": {
                        "ticker": {"type": "keyword"},
                        "year": {"type": "integer"},
                        "doc_type": {"type": "keyword"}
                    }
                }
            }
        }
    }

    # Create the index
    try:
        if not client.indices.exists(index=index_name):
            client.indices.create(index=index_name, body=index_body)
        else:
            print(f"Index '{index_name}' already exists. Skipping initialization.")
    except Exception as e:
        print(f"Error creating index: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--index", default="aapl_financials")
    args = parser.parse_args()

    initialize_index(args.endpoint, args.index)

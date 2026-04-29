import chromadb
client = chromadb.HttpClient(host="54.236.62.95", port=8000)
# This uses the client library to handle the URL routing for you
collection = client.get_collection(name="aapl_financials")
print(f"Collection ID: {collection.id}")
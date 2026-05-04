import chromadb
client = chromadb.HttpClient(host="44.213.90.18", port=8000)
# This uses the client library to handle the URL routing for you
collection = client.get_collection(name="aapl_financials")
print(f"Collection ID: {collection.id}")
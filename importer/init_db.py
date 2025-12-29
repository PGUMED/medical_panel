import json
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
DB_NAME = "hospital_db"
COLLECTION_NAME = "records"
DATA_FILE = "../data.json"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def init_db():
    collection.delete_many({})

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if data:
            collection.insert_many(data)
            print(f"Successfully imported {len(data)} records into MongoDB.")
        else:
            print("JSON file is empty.")
    else:
        print(f"File {DATA_FILE} not found.")


if __name__ == "__main__":
    init_db()

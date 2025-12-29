import json
import os
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['hospital_db']
collection = db['records']

DATA_FILE = 'data.json'


def init_db():
    # 1. Clear existing data (optional, prevents duplicates)
    collection.delete_many({})

    # 2. Load JSON
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if data:
            # 3. Insert into MongoDB
            collection.insert_many(data)
            print(f"Successfully imported {len(data)} records into MongoDB.")
        else:
            print("JSON file is empty.")
    else:
        print(f"File {DATA_FILE} not found.")


if __name__ == '__main__':
    init_db()
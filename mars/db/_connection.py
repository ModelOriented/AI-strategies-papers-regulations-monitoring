import os

import pyArango.collection
from dotenv import load_dotenv
from pyArango.connection import Connection
from pyArango.database import Database

load_dotenv()

conn = Connection(
    username=os.getenv("ARANGODB_USERNAME"),
    password=os.getenv("ARANGODB_PASSWORD"),
    arangoURL=os.getenv("ARANGODB_URL"),
)
try:
    database = conn.databases["mars"]  # type: Database
except KeyError:
    database = conn.createDatabase("mars")  # type: Database


def get_collection_or_create(
    collection_name: str, database=database
) -> pyArango.collection.Collection:
    try:
        return database[collection_name]
    except KeyError:
        return database.createCollection(name=collection_name)
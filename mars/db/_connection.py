import pyArango.collection
from dotenv import load_dotenv
from pyArango.connection import Connection
from pyArango.database import Database

from mars import config

load_dotenv()

conn = Connection(
    username=config.arango_username,
    password=config.arango_password,
    arangoURL=config.arango_url,
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

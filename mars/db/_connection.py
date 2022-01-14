import pyArango.collection
from mars import config
from pyArango.connection import Connection
from pyArango.database import Database

conn = Connection(
    username=config.arango_username,
    password=config.arango_password,
    arangoURL=config.arango_url,
)


try:
    database = conn.databases[config.arango_db_name]  # type: Database
except KeyError:
    database = conn.createDatabase(config.arango_db_name)  # type: Database


def get_collection_or_create(
    collection_name: str, database=database
) -> pyArango.collection.Collection:
    try:
        return database[collection_name]
    except KeyError:
        return database.createCollection(name=collection_name)

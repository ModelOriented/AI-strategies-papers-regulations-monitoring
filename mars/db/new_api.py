from arango import ArangoClient
from arango.http import DefaultHTTPClient
from mars import config


# Create client with no timeout
class NoTimeoutHttpClient(DefaultHTTPClient):
    """Extend the default arango http client, to remove timeouts for bulk data."""

    REQUEST_TIMEOUT = None


client = ArangoClient(hosts=config.arango_url, http_client=NoTimeoutHttpClient())

database = client.db(
    "mars", username=config.arango_username, password=config.arango_password
)

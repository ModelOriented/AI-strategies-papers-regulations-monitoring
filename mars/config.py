import os

from dotenv import load_dotenv

load_dotenv()

logging_level = os.getenv("LOGGING_LEVEL")
scrapper_logs_dir = os.getenv("SCRAPER_LOGS_DIR")
arango_username = os.getenv("ARANGODB_USERNAME")
arango_password = os.getenv("ARANGODB_PASSWORD")
arango_url = os.getenv("ARANGODB_URL")
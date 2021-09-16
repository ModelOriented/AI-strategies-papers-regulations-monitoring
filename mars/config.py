import os

from dotenv import load_dotenv

load_dotenv()

# Docker environment
load_dotenv('/run/secrets/arango_secrets')

logging_level = os.getenv("LOGGING_LEVEL")
scrapper_logs_dir = os.getenv("SCRAPER_LOGS_DIR")
arango_username = os.getenv("ARANGODB_USERNAME")
arango_password = os.getenv("ARANGODB_PASSWORD")
arango_url = os.getenv("ARANGODB_URL")
arango_database = os.getenv("ARANGODB_DATABASE")
raw_files_dir = os.getenv("RAW_FILES_DIR")
geckodriver_path = os.getenv("GECKODRIVER_PATH")
user = os.getenv("USER")

import os

from dotenv import load_dotenv

load_dotenv()

# Docker environment
load_dotenv("/run/secrets/arango_secrets")
load_dotenv("/run/secrets/webdav_secrets")
load_dotenv("/run/secrets/redis_secrets")

logging_level = os.getenv("LOGGING_LEVEL")
scrapper_logs_dir = os.getenv("SCRAPER_LOGS_DIR")
arango_username = os.getenv("ARANGODB_USERNAME")
arango_password = os.getenv("ARANGODB_PASSWORD")
arango_url = os.getenv("ARANGODB_URL")
arango_database = os.getenv("ARANGODB_DATABASE")
raw_files_dir = os.getenv("RAW_FILES_DIR")
tmp_files_dir = os.getenv("TMP_FILES_DIR") or "/tmp"
models_dir = os.getenv("MODELS_DIR")
data_dir = os.getenv("DATA_DIR")
geckodriver_path = os.getenv("GECKODRIVER_PATH")
user = os.getenv("USER")
webdav_login = os.getenv("WEBDAV_LOGIN") or "user"
webdav_password = os.getenv("WEBDAV_PASSWORD")
webdav_url = os.getenv("WEBDAV_ENDPOINT")
use_webdav = (os.getenv("USE_WEBDAV") or "0") in ["1", "True", "true", "TRUE"]
redis_url = os.getenv("REDIS_URL")

arango_db_name = os.getenv("ARANGODB_DB_NAME") or "mars"

import os

import mars.parser as parser
import dotenv
import mars.db as db
import pyArango

dotenv.load_dotenv()

parser.add_missing_files_to_db("data/ethics-ai-table")
parser.parse_source("SourceWebsite.localhost", 100)

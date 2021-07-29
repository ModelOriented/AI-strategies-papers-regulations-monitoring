import os

import mars.parser as parser
import dotenv
import mars.db as db
import pyArango

dotenv.load_dotenv()

parser.add_missing_files_to_db("data/ethics-ai-table")
# for doc in db.documentSources["SOURCE" == db.SourceWebsite.localhost]:
#   if doc["FILE_TYPE"] ==db.FileType.pdf:
#       parser.parse_pdf()
# if

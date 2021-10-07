"""Gets all segments from database without assigned language, recognize their language and saves it to database."""

import typer
from mars import logging
from mars.db import collections
from mars.db.db_fields import CONTENT, LANGUAGE
from mars.language_recognition import detect_language
from tqdm import tqdm

logger = logging.new_logger(__name__)
BATCH_SIZE = 1000


def main():
    query = collections.segmented_texts.fetchByExample(
        {LANGUAGE: None}, batchSize=BATCH_SIZE
    )
    print(f"Processing {query.count} sentences...")
    sections_to_update = []
    for section in tqdm(query, total=query.count):
        try:
            lang = detect_language(section[CONTENT])
            section[LANGUAGE] = lang
            sections_to_update.append(section)
            if len(sections_to_update) == BATCH_SIZE:
                collections.segmented_texts.bulkSave(
                    sections_to_update, onDuplicate="update"
                )
                sections_to_update = []
        except Exception as e:
            logging.log_exception("Exception:", e, logger)


if __name__ == "__main__":
    typer.run(main)

import typer
from mars import logging
from mars.db import collections
from mars.db.db_fields import CONTENT, LANGUAGE
from mars.language_recognition import detect_language
from tqdm import tqdm

logger = logging.new_logger(__name__)


def main():
    query = collections.segmented_texts.fetchByExample({LANGUAGE: None}, batchSize=100)
    print(f"Processing {query.count} sentences...")
    sections_to_update = []
    for section in tqdm(query, total=query.count):
        try:
            lang = detect_language(section[CONTENT])
            section[LANGUAGE] = lang
            sections_to_update.append(section)
        except Exception as e:
            logging.log_exception("Exception:", e, logger)
    print("Updating...")
    collections.segmented_texts.bulkSave(sections_to_update, onDuplicate="update")


if __name__ == "__main__":
    typer.run(main)

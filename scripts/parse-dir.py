import dotenv
import typer

import mars.db as db
import mars.parser as parser

dotenv.load_dotenv()


def main(dir_path: str):
    parser.add_missing_files_to_db(dir_path)
    parser.parse_source(db.SourceWebsite.manual, 100)


if __name__ == "__main__":
    typer.run(main)

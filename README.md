# AI-strategies-papers-regulations-monitoring
Monitoring of AI strategies, papers, and regulations

Code is available at: https://github.com/ModelOriented/AI-strategies-papers-regulations-monitoring (to access please contact [Stanisław Giziński](https://github.com/Gizzio) or [Hanna Zdulska](https://github.com/HaZdula))

### Project structure
* **secrets** contains credentials and donfiguration files for docker containers
  * **airflow.env[.example]** - credentials of airflow database
  * **arango.env[.example]** - credentials of arango database
  * **webdav.env[.example]** - credentials of webdav
  * **redis.env[.example]** - credentials of redis
  * **redis.conf[.example]** - configuration file of redis
* **mars** contains modules with code
  * **db** contains database connection abstraction layer
  * **config.py** contains configuration utils
  * **embeddings.py** contains code for generating embedding of sentences
  * **parser.py** contains code for extracting text from htmls and pdfs
  * **scraper.py** contains code for downloading documents from various source websites
  * **target_embeddigs.py** contains code embedding queries
* **scripts** contains data processing steps
  * **embedd_sentences.py**
  * **split_to_sentences.py**
  * **score_for_annotation.py** - code for setting scores to sentences accordingly to their annotation value
  * **split_to_sentences.py**
  * **parse-dir.py**
  * **parse_jobin2019.py** - parses jobin2019 and creates labels in mars/data folder
* **docker-compose.yml** - definition of services
* **annotation** - application for annotation
  * **app.py** - main script
  * **Dockerfile** - container definition for annotation app
* **scrapers** - scrapping scripts
  * **eurlex-scraping.py** - code for scrapping documents from eurlex
  * **oecd-scraping.py** - code for scrapping documents from oecd
  * **Dockerfile** - container definition for airflow scheduler for scrapers
  * **default.dag.py** - dag file for airflow

# Usage
## Pre-requirements
* docker
* docker-compose

## Configuration
#### Using docker-compose
Remove `.example` from secrets/*.example files and configure them with randomly generate passwords
#### Manual use
Move .env.example to .env and configure it

## Installation
`docker-compose pull`

## Running
### Inside docker
`docker-compose up`

## Testing
`poetry run python -m unittest discover`

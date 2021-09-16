# AI-stretegies-papers-regulations-monitoring
Monitoring of AI strategies, papers, and regulations

Code is available at: https://github.com/ModelOriented/AI-strategies-papers-regulations-monitoring (to access please contact [Stanisław Giziński](https://github.com/Gizzio) or [Hanna Zdulska](https://github.com/HaZdula))

### Project structure
* **airflow_secrets.env[.example]** - configuration file for credentials of airflow database
* **arango_secrets.env[.example]** - configuration file for credentials of arango database
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
Remove `.example` from *_secrets.env.example files and configure them with randomly generate passwords
## Installation
`docker-compose build`
## Running
### Inside docker
`docker-compose up`

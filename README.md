# AI-stretegies-papers-regulations-monitoring
Monitoring of AI strategies, papers, and regulations

Code is available at: https://github.com/ModelOriented/AI-strategies-papers-regulations-monitoring

### Project structure
* **mars** contains modules with code
  * **db** contains database connection abstraction layer
  * **config.py** contains configuratoin utils
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
  * **eurlex-scraping.py** - code for scrapping documents from eurlex
  * **oecd-scraping.py** - code for scrapping documents from oecd
* **docker-compose.yml** - definition of services
* **app.py** - applitacion for annotation

# Usage
## Pre-requirements
* docker
* docker-compose
## Installation
`docker-compose build`
## Running
### Inside docker
`docker-compose up`

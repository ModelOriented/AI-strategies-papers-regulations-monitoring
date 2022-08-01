**Files Description**

Unnecessary files:
* download_pdf.py and prepare_whole_table.py - artifacts that led to creation of Overton_download_parse_create_table.py script
* development_notebook_pdf_to_paragraphs.py - notebook with experiments

Crucial files:
* Overton_download_parse_create_table.py - script that downloads pdfs mentioned in Overton xlsx file, parses them into paragraphed and cleaned text and saves all into .parquet file
* Overton_Spacy.py - script that takes the output of the aforementioned file, prepares basic spacy objects: nouns, nounchunks and lemmas and saves it into .parquet file

Testing file:
* Overton_scripts_test.xlsx - small subset with 40 rows used for testing

To use both scripts one has to place them in a dedicated folder with xlsx file (example file is given). Firstly you have to download pdfs then run the script with preparing table. Whilst running scripts one has to provide (full) paths to requested folders. In the end folder structure should look like this:

![image](https://user-images.githubusercontent.com/56126542/180423400-d51b14e9-49e1-4682-8e75-d574196eb608.png)

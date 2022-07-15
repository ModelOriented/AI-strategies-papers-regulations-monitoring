To use both scripts one has to place them in a separate folder with xlsx file (example file is given). 
Firstly you have to download pdfs then run the script with preparing table. Whilst running scripts one has to provide (full) paths to requested folders. In the end folder structure should look like this:

example_folder

  **paragraphs**
  
    0.pdf
    
    1.pdf
    
  **pdfs**
  
    0.pdf
    
    1.pdf
    
    ...
    
    38.pdf
    
  missing.txt
  
  *Overton_download_pdf.py*
  
  *Overton_prepare_whole_table.pdf*
  
  *Overton_scripts_test.xlsx*
  
  progress.txt

*Italics* are necessery files
**Bolded** folders are just a suggestion, how it should look like
Not italic files will be generated automatically will be created automatically

Note - pdf converting to paragraphs has to be upgraded, because now it parses all documents at one without saves during the process

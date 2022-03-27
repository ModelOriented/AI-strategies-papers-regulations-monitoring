from bs4 import BeautifulSoup
import requests

white_paper_list=[['li', 'Heading1'],['li','ListParagraph']]

def segments_by_class(filename,class_list=[['li', 'ManualHeading3'], ['li', 'ManualConsidrant'],['Titrearticle']]):    #default class list for AI act 
    page = requests.get(filename)
    # parse html content
    soup = BeautifulSoup( page.content , 'html.parser')
    # get all tags
    tags = soup.find_all(["p"])
    par_list=[]
    chunk_of_text=''
    for i,tag in enumerate(tags) :
        
        # if tag has attribute of class
        if tag.has_attr( "class" ):
            if  tag['class'] in class_list:
                #end the segment, append it to the list and start new segment
                par_list.append(chunk_of_text)
                chunk_of_text=tag.get_text()
            elif tag['class'] == ['li', 'ManualHeading2'] and tags[i+1]['class'] != ['li', 'ManualHeading3']:
                #special case when tag from AI Act is the heading and no subheading follows it
                par_list.append(chunk_of_text)
                chunk_of_text=tag.get_text()
            else:
                #merge tag with the current segment
                chunk_of_text = chunk_of_text+tag.get_text()
      
    return( par_list )

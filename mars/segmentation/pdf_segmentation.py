import re
from operator import itemgetter
from typing import Tuple

import fitz


def count_fonts(doc: fitz.fitz.Document, round_digits: int = 1) -> Tuple[dict, dict]:
    """Extracts fonts and their usage in PDF documents.
    :param doc: PDF document to iterate through
    :type doc: <class 'fitz.fitz.Document'>
    :rtype: [(font_size, count), (font_size, count}], dict
    :return: most used fonts sorted by count, font style information
    """
    styles = {}
    font_counts = {}

    for page in doc:
        blocks = page.getText("dict")["blocks"]
        for b in blocks:  # iterate through the text blocks
            if b["type"] == 0:  # block contains text
                for l in b["lines"]:  # iterate through the text lines
                    for s in l["spans"]:  # iterate through the text spans
                        identifier = "{0}".format(round(s["size"], round_digits))
                        styles[identifier] = {
                            "size": round(s["size"], round_digits),
                            "font": s["font"],
                        }

                        font_counts[identifier] = (
                            font_counts.get(identifier, 0) + 1
                        )  # count the fonts usage

    font_counts = sorted(font_counts.items(), key=itemgetter(1), reverse=True)

    if len(font_counts) < 1:
        raise ValueError("Zero discriminating fonts found!")

    return font_counts, styles


def translate_font_to_tags(
    font_counts: dict, styles: dict, round_digits: int = 1
) -> dict:
    """Returns dictionary with font sizes as keys and tags as value.
    :param font_counts: (font_size, count) for all fonts occuring in document
    :type font_counts: list
    :param styles: all styles found in the document
    :type styles: dict
    :rtype: dict
    :return: all element tags based on font-sizes
    """
    p_style = styles[
        font_counts[0][0]
    ]  # get style for most used font by count (paragraph)
    p_size = p_style["size"]  # get the paragraph's size

    # sorting the font sizes high to low, so that we can append the right integer to each tag
    font_sizes = []
    for (font_size, count) in font_counts:
        font_sizes.append(round(float(font_size), round_digits))
    font_sizes.sort(reverse=True)

    # aggregating the tags for each font size
    idx = 0
    size_tag = {}
    for size in font_sizes:
        idx += 1
        if size == p_size:
            idx = 0
            size_tag[size] = "<p>"
        if size > p_size:
            size_tag[size] = "<h{0}>".format(idx)
        elif size < p_size:
            size_tag[size] = "<s>"  # .format(idx)

    return size_tag


def tag_headers(doc: fitz.fitz.Document, size_tag: dict, round_digits: int = 1):
    """Scrapes headers & paragraphs from PDF and return texts with element tags.
    :param doc: PDF document to iterate through
    :type doc: <class 'fitz.fitz.Document'>
    :param size_tag: textual element tags for each size
    :type size_tag: dict
    :rtype: list
    :return: texts with pre-prended element tags
    """
    header_para = []  # list with headers and paragraphs
    first = True  # boolean operator for first header
    previous_s = {}  # previous span

    for page in doc:
        blocks = page.getText("dict")["blocks"]
        for b in blocks:  # iterate through the text blocks
            if b["type"] == 0:  # this block contains text

                block_string = ""  # text found in block
                pipe = False

                for l in b["lines"]:  # iterate through the text lines
                    for s in l["spans"]:  # iterate through the text spans

                        tag = size_tag[round(s["size"], round_digits)]

                        if s["text"].strip():  # removing whitespaces:
                            if first:
                                previous_s = s
                                first = False
                                block_string = tag + s["text"].strip()
                            else:
                                if round(s["size"], round_digits) == round(
                                    previous_s["size"], round_digits
                                ):

                                    if (
                                        not block_string and pipe
                                    ):  # all((c == "|") for c in block_string):
                                        # block_string only contains pipes
                                        block_string = tag + s["text"].strip()
                                    if block_string == "":
                                        # new block has started, so append size tag
                                        block_string = tag + s["text"].strip()
                                    else:  # in the same block, so concatenate strings
                                        block_string += " " + s["text"].strip()

                                else:
                                    if block_string:
                                        header_para.append(block_string)
                                    block_string = tag + s["text"].strip()

                                previous_s = s

                    # new block started, indicating with a pipe
                    pipe = True
                    # block_string += "|"
                if block_string:
                    header_para.append(block_string)

    return header_para


def merge_spans(header_para):

    not_empty = [h for h in header_para if h]
    closed = []

    # fix to avoid errors out of range
    last_tag = re.findall(string=not_empty[0], pattern="<(.|..)>")[0]
    string_builder = re.sub("\<(.|..)>", " ", string=not_empty[0])

    counter = 0

    for h in not_empty[1:]:
        match = re.findall(pattern="\<(.|..)>", string=h)
        # todo rozdzielenie tagów
        if len(match) > 1:
            print("More than one tag detected")

        if len(match) == 0:
            return []
        current_tag = match[0]

        if current_tag == "s" and last_tag == "p":
            current_tag = "p"
        if current_tag == last_tag:
            string_builder += re.sub("\<(.|..)>", " ", string=h)
        else:
            closed.append(
                {
                    "html_tag": last_tag,
                    "content": string_builder,
                    "sequence_number": counter,
                }
            )
            counter += 1
            string_builder = re.sub("\<(.|..)>", " ", string=h)

        last_tag = current_tag
    return closed


def segment_pdf(filename, round_digits=1):
    """
    splits pdf file into headers and paragraphs
    @param round_digits - how many digits should be rounded while counting fonts

    @returns list of dicts {"html_tag": string, "content": string}
    """
    doc = fitz.open(filename=filename)
    font_counts, styles = count_fonts(doc, round_digits=round_digits)
    size_tag = translate_font_to_tags(font_counts, styles, round_digits=round_digits)
    arr = tag_headers(doc, size_tag, round_digits=round_digits)
    segments = merge_spans(arr)

    return segments

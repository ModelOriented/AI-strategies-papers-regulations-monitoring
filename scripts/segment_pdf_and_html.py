"""Split all pdfs from database"""
import typer
import mars.segmentation.segmantation as segmentation
import mars.db

if __name__ == "__main__":
    min_key_query = f"FOR u IN {mars.db.collections.SEGMENTED_TEXTS} SORT TO_NUMBER(u._key) ASC LIMIT 1 RETURN TO_NUMBER(u._key)"
    max_key_query = f"FOR u IN {mars.db.collections.SEGMENTED_TEXTS} SORT TO_NUMBER(u._key) DESC LIMIT 1 RETURN TO_NUMBER(u._key)"
    min_key = mars.db.database.AQLQuery(min_key_query, 1, rawResults=True)[0]
    max_key = mars.db.database.AQLQuery(max_key_query, 1, rawResults=True)[0]
    print(min_key, max_key)
    typer.run(segmentation.segment_and_upload(key_min=min_key, key_max=max_key))

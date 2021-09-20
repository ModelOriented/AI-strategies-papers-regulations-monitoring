"""Adds embeddings to all processed texts"""

import typer

import mars
from mars import db
from mars.db.db_fields import EMBEDDINGS, ID, EmbeddingType

BATCH_SIZE = 1000

unique_texts_query = f"""FOR doc IN {db.collections.SENTENCES} RETURN DISTINCT doc.{db.db_fields.TEXT_ID}"""


def embedd_sentences(embedding_type: EmbeddingType) -> None:
    from mars import (
        embeddings,
    )  # we import here to avoid huge model loading, before checking if parameters are valid

    unique_texts_id = mars.db.database.AQLQuery(unique_texts_query, BATCH_SIZE)

    if embedding_type == EmbeddingType.LABSE:
        embed_function = embeddings.embedd_sents_labse
    elif embedding_type == EmbeddingType.LASER:
        embed_function = embeddings.embedd_sents_laser

    for doc in db.collections.sentences.fetchAll():
        if not doc[EMBEDDINGS]:
            doc[EMBEDDINGS] = dict()
        if not doc[EMBEDDINGS][embedding_type]:
            print("Processing", doc[ID], "...")
            try:
                full_embeddings = embed_function(doc.sentence)
                doc[EMBEDDINGS][embedding_type] = full_embeddings
                doc.save()
            except Exception as e:
                print("Exception occurred in processing", doc[ID])
                print(e)
        else:
            print("Skipping", doc[ID], embedding_type)


if __name__ == "__main__":
    typer.run(embedd_sentences)

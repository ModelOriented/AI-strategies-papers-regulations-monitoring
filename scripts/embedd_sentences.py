"""Adds embeddings to all processed texts"""

import typer
from mars.db.db_fields import EMBEDDINGS, ID

BATCH_SIZE = 1000


def embedd_sentences(embedding_type: str) -> None:
    from mars import db, embeddings

    if embedding_type == "labse":
        embedd = embeddings.embedd_sents_labse
    elif embedding_type == "laser":
        embedd = embeddings.embedd_sents_laser
    else:
        raise ValueError(f"Unknown embedding type: {embedding_type}")
    for doc in db.collections.processed_texts.fetchAll():
        full_embbedings = list()
        if not doc[EMBEDDINGS]:
            doc[EMBEDDINGS] = dict()
        if not doc[EMBEDDINGS][embedding_type]:
            print("Processing", doc[ID], "...")
            try:
                if len(doc.sentences) == 0:
                    raise Exception("Empty array")
                for i in range(0, len(doc.sentences), BATCH_SIZE):
                    batch_embeddings = embedd(doc.sentences[i : i + BATCH_SIZE])
                    full_embbedings += batch_embeddings.tolist()
                doc[EMBEDDINGS][embedding_type] = full_embbedings
                doc.save()
            except Exception as e:
                print("Exception occured in processing", doc[ID])
                print(e)
        else:
            print("Skipping", doc[ID], embedding_type)


if __name__ == "__main__":
    typer.run(embedd_sentences)

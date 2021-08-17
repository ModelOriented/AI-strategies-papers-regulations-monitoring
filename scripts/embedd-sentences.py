from mars import db, embeddings

BATCH_SIZE = 1000


for doc in db.collections.processed_texts.fetchAll():
    full_embbedings = list()
    if not doc["sentencesEmbeddings"]:
        print("Processing", doc["_id"], "...")
        try:
            if len(doc.sentences) == 0:
                raise Exception("Empty array")
            for i in range(0, len(doc.sentences), BATCH_SIZE):
                batch_embeddings = embeddings.embedd_sents(
                    doc.sentences[i : i + BATCH_SIZE]
                )
                full_embbedings += batch_embeddings.numpy().tolist()
            doc["sentencesEmbeddings"] = full_embbedings
            doc.save()
        except Exception as e:
            print("Exception occured in processing", doc["_id"])
            print(e)
    else:
        print("Skipping", doc["_id"])

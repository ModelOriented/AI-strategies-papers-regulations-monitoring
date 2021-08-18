from mars.db import get_collection_or_create

processed_texts = get_collection_or_create("ProcessedTexts")
document_sources = get_collection_or_create("Documents")
contents = get_collection_or_create("Texts")
search_targets = get_collection_or_create("SearchTargets")

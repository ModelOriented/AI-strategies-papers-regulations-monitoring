from mars.db import get_collection_or_create

PROCESSED_TEXTS = "ProcessedTexts"
DOCUMENTS = "Documents"
CONTENTS = "Texts"
SEARCH_TARGETS = "SearchTargets"
ANNOTATIONS = "Annotations"
ANALYTICS = "Analytics"
SEGMENTED_TEXTS = "SegmentedTexts"
SENTENCES = "Sentences"

processed_texts = get_collection_or_create(PROCESSED_TEXTS)
document_sources = get_collection_or_create(DOCUMENTS)
contents = get_collection_or_create(CONTENTS)
search_targets = get_collection_or_create(SEARCH_TARGETS)
annotations = get_collection_or_create(ANNOTATIONS)
analytics = get_collection_or_create(ANALYTICS)
segmented_texts = get_collection_or_create(SEGMENTED_TEXTS)
sentences = get_collection_or_create(SENTENCES)

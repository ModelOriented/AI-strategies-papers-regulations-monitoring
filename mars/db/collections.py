from mars.db import get_collection_or_create

PROCESSED_TEXTS = "ProcessedTexts"
DOCUMENTS = "Documents"
CONTENTS = "Texts"
SEARCH_TARGETS = "SearchTargets"
ANNOTATIONS = "Annotations"
SEGMENTED_TEXTS = "SegmentedTexts"

processed_texts = get_collection_or_create(PROCESSED_TEXTS)
document_sources = get_collection_or_create(DOCUMENTS)
contents = get_collection_or_create(CONTENTS)
search_targets = get_collection_or_create(SEARCH_TARGETS)
annotations = get_collection_or_create(ANNOTATIONS)
segmented_texts = get_collection_or_create(SEGMENTED_TEXTS)

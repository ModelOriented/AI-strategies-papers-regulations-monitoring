from mars import db, embeddings

targets = [
    "privacy protection",
    "fairness",
    "accountability",
    "transparency, openness",
    "safety, cybersecurity",
    "common good, sustainability, well-being",
    "human oversight, control, auditng",
    "solidarity, inclusion, social cohesion",
    "explainability, interpretabiliy",
    "science-policy link",
    "legislative framework, legal status of AI systems",
    "future of employment/worker rights",
    "responsible/intensified research funding",
    "public awareness, education about AI and its risks",
    "dual-use problem, military, AI arms race",
    "field-specific deliberations (health, military, mobility etc.)",
    "human autonomy",
    "diversity in the field of AI",
    "certification for AI products",
    "protection of whistleblowers",
    "cultural differences in the ethically aligned design of AI systems",
    "hidden costs (labeling, clickwork, contend moderation, energy, resources)",
]


def get_sentence_to_embedding_mapping(targets: list, emb_type: str = "labse") -> dict:
    if emb_type == "labse":
        embds = embeddings.embedd_sents_labse(targets)
    elif emb_type == "laser":
        embds = embeddings.embedd_sents_laser(targets)
    else:
        raise ValueError("Unknown embedding type")
    target_embeddings = dict()
    for emb, targ in zip(embds, targets):
        target_embeddings[targ] = emb.numpy()
    return target_embeddings

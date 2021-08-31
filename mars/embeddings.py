import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # Needed for loading universal-sentence-encoder-cmlm/multilingual-preprocess
from laserembeddings import Laser

laser = Laser()

preprocessor = hub.KerasLayer(
    "https://tfhub.dev/google/universal-sentence-encoder-cmlm/multilingual-preprocess/2"
)
encoder = hub.KerasLayer("https://tfhub.dev/google/LaBSE/2")


def normalization(embeds):
    norms = np.linalg.norm(embeds, 2, axis=1, keepdims=True)
    return embeds / norms


def embedd_sents_labse(sents):
    return normalization(encoder(preprocessor(tf.constant(sents)))["default"]).numpy()


def embedd_sents_laser(sents):
    return normalization(laser.embed_sentences(sents, lang="en"))


def get_sentence_to_embedding_mapping(sentences: list, emb_type: str = "labse") -> dict:
    if emb_type == "labse":
        embds = embedd_sents_labse(sentences)
    elif emb_type == "laser":
        embds = embedd_sents_laser(sentences)
    else:
        raise ValueError("Unknown embedding type")
    target_embeddings = dict()
    for emb, targ in zip(embds, sentences):
        target_embeddings[targ] = emb.numpy()
    return target_embeddings

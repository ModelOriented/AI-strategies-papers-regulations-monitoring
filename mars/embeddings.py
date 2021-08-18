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
    return normalization(encoder(preprocessor(tf.constant(sents)))["default"])


def embedd_sents_laser(sents):
    return normalization(laser.embed_sentences(sents, lang="en"))

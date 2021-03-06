from abc import ABC, abstractmethod

import numpy as np
import tensorflow as tf
from transformers import AutoTokenizer
from transformers import TFDistilBertForSequenceClassification


class AbstractDefinitionModel(ABC):

    @abstractmethod
    def predict_single_sentence(self):
        pass


class DistilBertBaseUncased(AbstractDefinitionModel):

    def __init__(self, path_to_model: str, path_to_tokenizer: str):
        self.model = TFDistilBertForSequenceClassification.from_pretrained(path_to_model)
        self.tokenizer = AutoTokenizer.from_pretrained(path_to_tokenizer)

    def predict_single_sentence(self, sentence: str) -> float:
        """
        Predict probabilistic score of being a definition
        @param sentence: a single sentence in form of string
        @return: probabilistic output of being a definition
        """
        def predict_tokens(inputs):
            inputs["labels"] = tf.reshape(tf.constant(1), (-1, 1))  # Batch size 1
            outputs = self.model(inputs)
            predictions = tf.math.softmax(outputs.logits, axis=-1)
            return float(np.array(predictions)[0][1])

        inputs = self.tokenizer(sentence, return_tensors="tf", truncation=True)
        preds = predict_tokens(inputs)

        return preds
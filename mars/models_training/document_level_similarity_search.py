import numpy as np
from sklearn.feature_selection import RFE, RFECV, VarianceThreshold
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import FunctionTransformer


def bucketize(scores, bins):
    return np.stack([np.histogram(x, range=(-1, 1), bins=bins)[0] for x in scores])


def normalize(X_bucketized: np.ndarray):
    return X_bucketized / X_bucketized.sum(axis=1)[:, np.newaxis]


model = make_pipeline(
    FunctionTransformer(bucketize, validate=False, kw_args={"bins": 50}),
    VarianceThreshold(threshold=0),
    RFECV((LogisticRegression(penalty="l1", solver="liblinear")), step=1, cv=3),
    LogisticRegression(solver="liblinear"),
)

model_with_normalization = make_pipeline(
    FunctionTransformer(bucketize, validate=False, kw_args={"bins": 50}),
    FunctionTransformer(normalize),
    VarianceThreshold(threshold=0),
    RFECV((LogisticRegression(penalty="l1", solver="liblinear")), step=1, cv=3),
    LogisticRegression(solver="liblinear"),
)

model_with_normalization_rfe = Pipeline(
    [
        (
            "bucketize",
            FunctionTransformer(bucketize, validate=False, kw_args={"bins": 50}),
        ),
        ("normalize", FunctionTransformer(normalize)),
        ("variance_feature_selectiion", VarianceThreshold(threshold=0)),
        (
            "rfe",
            RFE(
                (LogisticRegression(penalty="l1", solver="liblinear")),
                step=1,
                n_features_to_select=50,
            ),
        ),
        ("model", LogisticRegression(solver="liblinear")),
    ]
)

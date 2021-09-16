import numpy as np
import scikitplot.metrics
import sklearn.metrics


def evaluate(y_pred: np.ndarray, y: np.ndarray):
    scikitplot.metrics.plot_roc(
        y,
        np.stack([1 - np.array(y_pred), np.array(y_pred)], axis=1),
        plot_micro=False,
        plot_macro=False,
        classes_to_plot=1,
    )
    print("AUC ROC:", sklearn.metrics.roc_auc_score(y, y_pred))

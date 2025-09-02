import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def conf_matrix(df_1, df_2):
    """
    Build confusion matrix manually and compute precision, recall, f1-score, and accuracy.
    """
    if df_1.shape != df_2.shape:
        print("Shape of data is not equal")
        return

    classes = pd.Index(df_1['knight'].unique()).union(df_2['knight'].unique())
    n = len(classes)

    matrix = np.zeros((n, n), dtype=int)

    for true, pred in zip(df_1['knight'], df_2['knight']):
        i = np.where(classes == true)[0][0]
        j = np.where(classes == pred)[0][0]
        matrix[i, j] += 1

    metrics = []
    for k, cls in enumerate(classes):
        TP = matrix[k, k]
        FP = matrix[:, k].sum() - TP
        FN = matrix[k, :].sum() - TP

        precision = TP / (TP + FP) if (TP + FP) != 0 else 0
        recall = TP / (TP + FN) if (TP + FN) != 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) != 0 else 0
        total = matrix[k, :].sum()

        metrics.append([round(precision, 2), round(recall, 2), round(f1, 2), total])

    df_metrics = pd.DataFrame(metrics,
                              index=classes,
                              columns=["precision", "recall", "f1-score", "total"])

    accuracy = round(np.trace(matrix) / matrix.sum(), 2)
    total = matrix.sum()
    df_metrics.loc["accuracy"] = ["", "", accuracy, total]

    print(df_metrics)
    print(matrix)

    plt.figure(figsize=(6, 5))
    plt.imshow(matrix, cmap="coolwarm")
    plt.xticks(range(n), classes)
    plt.yticks(range(n), classes)
    plt.colorbar()
    plt.title("Confusion Matrix")
    for i in range(n):
        for j in range(n):
            plt.text(j, i, matrix[i, j], ha='center', va='center', color='black')
    plt.show()


def main():
    """
    Load truth and prediction files, then display confusion matrix and metrics.
    """
    truth_path = "../data/truth.txt"
    prediction_path = "../data/predictions.txt"
    df_truth = pd.read_csv(truth_path, header=None, names=['knight'])
    df_predictions = pd.read_csv(prediction_path, header=None, names=['knight'])
    conf_matrix(df_truth, df_predictions)


if __name__ == "__main__":
    main()

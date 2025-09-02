import pandas as pd
import sys
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import f1_score, accuracy_score
import matplotlib.pyplot as plt

sys.path.append("../../DS03/ex05")
from split import split


def visualize_KNN(k_values, acc_list):
    """
    Display a line graph of accuracy (or F1-score) versus different k values for KNN.
    """
    plt.figure(figsize=(8,5))
    plt.plot(k_values, acc_list)
    plt.title("Accuracy vs K")
    plt.xlabel("k values")
    plt.ylabel("accuracy")
    plt.grid(True)
    plt.show()


def train_model(df_train, k):
    """
    Train a K-Nearest Neighbors classifier on the provided training data.
    """
    X_train = df_train.drop(columns="knight")
    y_train = df_train["knight"]

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    knn = KNeighborsClassifier(n_neighbors=k)
    knn.fit(X_train_scaled, y_train)

    return knn, scaler


def predict_model(knn, df_test):
    """
    Predict labels using a trained KNN model and save predictions to 'KNN.txt'.
    """
    predictions = knn.predict(df_test)
    with open("KNN.txt", "w") as f:
        for p in predictions:
            f.write(p + "\n")


def evaluate_model(df):
    """
    Split the dataset into training and validation sets, evaluate KNN models for k=1..30,
    calculate accuracy and F1-score, and visualize the results.
    """
    split(df, 0.8)
    df_train = pd.read_csv("Training_knight.csv")
    df_val = pd.read_csv("Validation_knight.csv")

    scaler = StandardScaler()

    X_train = df_train.drop(columns="knight")
    y_train = df_train["knight"]

    X_val = df_val.drop(columns="knight")
    y_val = df_val["knight"]

    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    f1_scores = []
    acc_list = []
    k_values = range(1, 30)

    for k in k_values:
        knn = KNeighborsClassifier(n_neighbors=k)
        knn.fit(X_train_scaled, y_train)
        y_pred = knn.predict(X_val_scaled)
        f1 = f1_score(y_val, y_pred, average="weighted")
        acc = accuracy_score(y_val, y_pred)
        acc_list.append(acc)
        f1_scores.append(f1)

    visualize_KNN(k_values, acc_list)

    max_f1 = max(acc_list)
    best_k = acc_list.index(max_f1)
    print(f"Accuracy for k = {best_k + 1} is {acc_list[best_k]}")
    print(f"F1-score for k = {best_k + 1} is {f1_scores[best_k]}")
    return best_k + 1


def main():
    """
    Main function to run the KNN pipeline:
    - Load train and test datasets
    - Evaluate KNN to find best k
    - Train final KNN on all training data
    - Scale test data and predict
    - Save predictions to 'KNN.txt'
    """
    if len(sys.argv) != 3:
        raise ValueError("Usage: python KNN.py <Train_knight.csv> <Test_knight.csv>")

    path_train, path_test = sys.argv[1], sys.argv[2]
    df_train = pd.read_csv(path_train)
    df_test = pd.read_csv(path_test)

    k= evaluate_model(df_train)

    knn, scaler = train_model(df_train, k)

    X_test_scaled = scaler.transform(df_test)
    predict_model(knn, X_test_scaled)


if __name__ == "__main__":
    main()

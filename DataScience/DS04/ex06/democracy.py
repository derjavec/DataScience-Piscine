import pandas as pd
import sys
from collections import Counter
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score

sys.path.append("../../DS03/ex05")
from split import split


def train_classifier(df_train, model_type):
    """
    Train a classifier of the given type on the training dataset.
    """
    X_train = df_train.drop(columns="knight")
    y_train = df_train["knight"]

    scaler = None
    if model_type in ["knn", "logistic_regression"]:
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)

    if model_type == "decision_tree":
        clf = DecisionTreeClassifier(random_state=42)
    elif model_type == "random_forest":
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
    elif model_type == "knn":
        k = find_best_k(df_train)
        clf = KNeighborsClassifier(n_neighbors=k)
    elif model_type == "logistic_regression":
        clf = LogisticRegression(max_iter=1000)
    else:
        raise ValueError("Invalid model_type. Choose a valid model type.")

    clf.fit(X_train, y_train)
    return clf, scaler


def make_predictions(clf, df_test, scaler=None):
    """
    Generate predictions using a trained classifier.
    """
    X_test = df_test
    if scaler is not None:
        X_test = scaler.transform(X_test)
    return clf.predict(X_test)


def find_best_k(df):
    """
    Determine the optimal number of neighbors (k) for KNN using validation F1-score.
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
    k_values = range(1, 31)

    for k in k_values:
        knn = KNeighborsClassifier(n_neighbors=k)
        knn.fit(X_train_scaled, y_train)
        y_pred = knn.predict(X_val_scaled)
        f1 = f1_score(y_val, y_pred, average="weighted")
        f1_scores.append(f1)

    best_k = f1_scores.index(max(f1_scores)) + 1
    return best_k


def majority_voting(predictions):
    """
    Perform majority voting across predictions from multiple classifiers.
    """
    votes = []
    for pred in zip(*predictions):
        counts = Counter(pred)
        winner = counts.most_common(1)[0][0]
        votes.append(winner)

    with open("Voting.txt", "w") as f:
        for v in votes:
            f.write(v + "\n")
    return votes


def generate_model_predictions(df_train, df_test):
    """
    Train multiple classifiers and generate predictions for the test set.
    """
    models = ["decision_tree", "knn", "logistic_regression"]
    predictions = []
    for model in models:
        clf, scaler = train_classifier(df_train, model_type=model)
        pred = make_predictions(clf, df_test, scaler)
        predictions.append(pred)
    return predictions


def evaluate_voting_ensemble(df):
    """
    Evaluate an ensemble of classifiers using majority voting on a validation set.

    Args:
        df (pd.DataFrame): Full dataset to split into training and validation.

    Returns:
        None: Prints weighted F1 score.
    """
    split(df, 0.8)
    df_train = pd.read_csv("Training_knight.csv")
    df_val = pd.read_csv("Validation_knight.csv")

    X_val = df_val.drop(columns="knight")
    y_val = df_val["knight"]

    predictions = generate_model_predictions(df_train, X_val)
    y_pred = majority_voting(predictions)
    score = f1_score(y_val, y_pred, average="weighted")
    print(f"Weighted F1 Score: {score:.4f}")


def main():
    """
    Main function to train models, apply majority voting, and evaluate ensemble performance.
    """
    if len(sys.argv) != 3:
        raise ValueError("Usage: python democracy.py <Train_knight.csv> <Test_knight.csv>")

    path_train, path_test = sys.argv[1], sys.argv[2]
    df_train = pd.read_csv(path_train)
    df_test = pd.read_csv(path_test)

    predictions = generate_model_predictions(df_train, df_test)
    majority_voting(predictions)
    evaluate_voting_ensemble(df_train)


if __name__ == "__main__":
    main()

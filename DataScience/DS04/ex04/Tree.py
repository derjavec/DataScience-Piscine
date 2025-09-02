import pandas as pd
import sys
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score
import matplotlib.pyplot as plt

sys.path.append("../../DS03/ex05")
from split import split


def visualize_tree(clf, X_train):
    """
    Display a decision tree graph.
    """
    plt.figure(figsize=(20, 10))
    plot_tree(
        clf,
        feature_names=X_train.columns,
        class_names=clf.classes_,
        filled=True,
        rounded=True,
        fontsize=12
    )
    plt.show()


def train_model(df_train, model_type="decision_tree"):
    """
    Train a classifier and return the trained model.
    """
    X_train = df_train.drop(columns="knight")
    y_train = df_train["knight"]

    if model_type == "decision_tree":
        clf = DecisionTreeClassifier(random_state=42)
        clf.fit(X_train, y_train)
        visualize_tree(clf, X_train)
    elif model_type == "random_forest":
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X_train, y_train)
    else:
        raise ValueError("Invalid model_type. Choose 'decision_tree' or 'random_forest'.")

    return clf


def predict_model(clf, df_test):
    """
    Predict labels using a trained classifier.
    """
    predictions = clf.predict(df_test)
    with open("Tree.txt", "w") as f:
        for p in predictions:
            f.write(p + "\n")
    return predictions


def evaluate_model(df, model_type="random_forest", average_type="weighted"):
    """
    Split df into training and validation, train a model, and calculate F1-score.
    """
    split(df, 0.8)
    df_train = pd.read_csv("Training_knight.csv")
    df_val = pd.read_csv("Validation_knight.csv")

    clf = train_model(df_train, model_type=model_type)

    X_val = df_val.drop(columns="knight")
    y_val = df_val["knight"]
    y_pred = clf.predict(X_val)

    score = f1_score(y_val, y_pred, average=average_type)
    print(f"Model: {model_type}, Average: {average_type}, Weighted F1 Score: {score:.4f}")


def main():
    """
    Main function to evaluate all combinations of models and F1-score averages.
    """
    if len(sys.argv) != 3:
        raise ValueError("Usage: python Tree.py <Train_knight.csv> <Test_knight.csv>")

    path_train, path_test = sys.argv[1], sys.argv[2]
    df_train = pd.read_csv(path_train)
    df_test = pd.read_csv(path_test)

    clf_dt = train_model(df_train, model_type="decision_tree")
    predict_model(clf_dt, df_test)

    models = ["random_forest"]
    averages = ["macro", "micro", "weighted"]

    for model in models:
        for avg in averages:
            evaluate_model(df_train, model_type=model, average_type=avg)


if __name__ == "__main__":
    main()

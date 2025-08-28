import pandas as pd
import sys
from sklearn.tree import DecisionTreeClassifier, plot_tree
import matplotlib.pyplot as plt
from sklearn.metrics import f1_score


def plot(clf, x_train):
    plt.figure(figsize=(20,10))
    plot_tree(
        clf, 
        feature_names=x_train.columns,
        class_names=clf.classes_,
        filled=True,
        rounded=True,
        fontsize=12
    )
    plt.show()


def main():
    """
    Load the dataset and 
    """
    assert len(sys.argv) == 3 , "Wrong amount of arguments"
    assert sys.argv[1] == "../data/Train_knight.csv", "First argument must be the path of Train_knight.csv. If nobody move it the path is: ../data/Train_knight.csv"
    assert sys.argv[2] == "../data/Test_knight.csv", "First argument must be the path of Test_knight.csv. If nobody move it the path is: ../data/Test_knight.csv"

    path_train = sys.argv[1]
    path_test = sys.argv[2]
    
    df_train = pd.read_csv(path_train)
    df_test = pd.read_csv(path_test)

    x_train = df_train.drop(columns = "knight")
    y_train = df_train['knight']
    clf = DecisionTreeClassifier()
    clf.fit(x_train, y_train)
    plot(clf, x_train)
    prediction = clf.predict(df_test)
    print(prediction)

    with open("Tree.txt", "w") as f:
        for p in prediction:
            f.write(p + "\n")
    
    # y_pred = clf.predict(x_train)
    # score = f1_score(y_train, y_pred, average="weighted")
    # print(score)
    

if __name__ == "__main__":
    main()
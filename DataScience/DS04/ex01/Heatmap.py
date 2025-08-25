import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def plot_heat_map(df):
    """
    Plot a heatmap of the correlation coefficient between numerical columns.
    """
    df_numeric = df.select_dtypes(include='number')
    
    corr_matrix = df_numeric.corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=False, fmt=".2f", cmap="coolwarm", cbar=True)
    # plt.xticks([])
    # plt.yticks([])
    plt.title("Correlation Coefficient Heatmap")
    plt.show()


def main():
    """
    Load the dataset and plot the correlation heatmap.
    """
    path = "../data/Train_knight.csv"
    
    df = pd.read_csv(path)
    
    plot_heat_map(df)


if __name__ == "__main__":
    main()

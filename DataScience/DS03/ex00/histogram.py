import pandas as pd
import matplotlib.pyplot as plt
import math


def plot_all_histograms(df):
    """
    Plot all histograms of the dataframe in a single figure with subplots.
    X = value of attribute, Y = frequency (number of knights)
    """
    cols = df.columns
    n_cols = 5
    n_rows = math.ceil(len(cols) / n_cols)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols*4, n_rows*3))
    axes = axes.flatten()

    for i, col in enumerate(cols):
        axes[i].hist(df[col], edgecolor='black', color='green', alpha=0.5)
        axes[i].set_title(col, fontsize=10)

    for j in range(i+1, len(axes)):
        axes[j].set_visible(False)

    plt.tight_layout()
    plt.show()


def plot_vs_histograms(df):
    """
    Plot histograms for each attribute, separated by knight categories.
    Different knights are displayed with different colors.
    """
    cols = df.columns
    n_cols = 5
    n_rows = math.ceil(len(cols) / n_cols)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols*4, n_rows*3))
    axes = axes.flatten()
    knight_names = df['knight'].unique()

    knight_dfs = {}     
    for name in knight_names:
        knight_dfs[name] = df[df['knight'] == name]

    colors = plt.cm.tab10.colors 
    
    for i, col in enumerate(cols):
        for j, (name, df) in enumerate(knight_dfs.items()):
            axes[i].hist(df[col], edgecolor='black', color=colors[j % len(colors)], alpha=0.5, label = name)
            axes[i].set_title(col, fontsize=10)

    for j in range(i+1, len(axes)):
        axes[j].set_visible(False)

    plt.tight_layout()
    plt.show()


def main():
    """
    Load knight datasets and plot histograms.
    - For the test dataset: plots overall attribute distributions.
    - For the training dataset: plots attribute distributions separated by knight.
    """
    csv_path_1 = "../data/Test_knight.csv"
    csv_path_2 = "../data/Train_knight.csv"

    df_1 = pd.read_csv(csv_path_1)
    plot_all_histograms(df_1)
    df_2 = pd.read_csv(csv_path_2)
    plot_vs_histograms(df_2)


if __name__ == "__main__":
    main()

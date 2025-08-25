import pandas as pd
import matplotlib.pyplot as plt

def plot_points(df_1, df_2):
    """
    Plot 4 scatter plots comparing numeric features.
    df_1: two scatter plots in green.
    df_2: two scatter plots colored by knight.
    """
    n_cols = 2
    n_rows = 2
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols*4, n_rows*3))
    axes = axes.flatten()

    cols_1 = df_1.columns[:4]
    cols_2 = df_2.columns[:4]

    axes[0].scatter(df_1[cols_1[0]], df_1[cols_1[1]], color='green', alpha=0.5)
    axes[0].set_title(f"{cols_1[0]} vs. {cols_1[1]}", fontsize=10)

    axes[1].scatter(df_1[cols_1[2]], df_1[cols_1[3]], color='green', alpha=0.5)
    axes[1].set_title(f"{cols_1[2]} vs. {cols_1[3]}", fontsize=10)

    knight_names = df_2['knight'].unique()
    knight_dfs = {name: df_2[df_2['knight'] == name] for name in knight_names}
    colors = plt.cm.tab10.colors

    for j, (name, subdf) in enumerate(knight_dfs.items()):
        axes[2].scatter(subdf[cols_2[0]], subdf[cols_2[1]],
                        color=colors[j % len(colors)], alpha=0.5, label=name)
    axes[2].set_title(f"{cols_2[0]} vs. {cols_2[1]}", fontsize=10)
    axes[2].legend(fontsize=8)

    for j, (name, subdf) in enumerate(knight_dfs.items()):
        axes[3].scatter(subdf[cols_2[2]], subdf[cols_2[3]],
                        color=colors[j % len(colors)], alpha=0.5, label=name)
    axes[3].set_title(f"{cols_2[2]} vs. {cols_2[3]}", fontsize=10)
    axes[3].legend(fontsize=8)

    plt.tight_layout()
    plt.show()

def main():
    """
    Load datasets and plot scatter plots.
    """
    csv_path_1 = "../data/Test_knight.csv"
    csv_path_2 = "../data/Train_knight.csv"

    df_1 = pd.read_csv(csv_path_1)
    df_2 = pd.read_csv(csv_path_2)
    plot_points(df_1, df_2)

if __name__ == "__main__":
    main()

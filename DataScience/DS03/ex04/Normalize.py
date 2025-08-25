import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

def print_and_plot_normalized_values(df):
    """
    Normalize numerical columns and print them.
    Plot the first two columns as a scatter plot.
    If 'knight' exists, color points by knight.
    """
    df_num = df.select_dtypes(include='number')
    scaler = MinMaxScaler()

    df_num_norm = pd.DataFrame(scaler.fit_transform(df_num), columns=df_num.columns)

    if 'knight' in df.columns:
        df_num_norm['knight'] = df['knight'].values

    print(df_num_norm.to_string(index=False))

    cols = df_num_norm.columns[:2]

    if 'knight' in df_num_norm.columns:
        knight_names = df_num_norm['knight'].unique()
        knight_dfs = {name: df_num_norm[df_num_norm['knight'] == name] for name in knight_names}
        colors = plt.cm.tab10.colors
        for j, (name, subdf) in enumerate(knight_dfs.items()):
            plt.scatter(subdf[cols[0]], subdf[cols[1]],
                        color=colors[j % len(colors)], alpha=0.5, label=name)
        plt.legend()
    else:
        plt.scatter(df_num_norm[cols[0]], df_num_norm[cols[1]], color='green', alpha=0.5)

    plt.title(f"{cols[0]} vs. {cols[1]}", fontsize=10)
    plt.tight_layout()
    plt.show()


def main():
    """
    Load datasets and normalize numeric columns.
    Plot the first two features as a scatter plot.
    """
    csv_path_1 = "../data/Test_knight.csv"
    csv_path_2 = "../data/Train_knight.csv"
    df_1 = pd.read_csv(csv_path_1)
    df_2 = pd.read_csv(csv_path_2)
    print_and_plot_normalized_values(df_1)
    print_and_plot_normalized_values(df_2)


if __name__ == "__main__":
    main()

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

def mean(data):
    """
    Calculate the mean (average) of a list of numbers.
    """
    return sum(data) / len(data)


def variance(data_sorted):
    """
    Calculate the variance of a list of numbers.
    """
    n = len(data_sorted)
    m = mean(data_sorted)
    return sum((x - m)**2 for x in data_sorted) / n


def plot_variance(df):
    """
    Perform PCA on numeric features of the dataframe, 
    compute cumulative explained variance, and plot it.
    
    Behavior:
        - Scales numeric features to zero mean and unit variance.
        - Computes principal components using PCA.
        - Plots cumulative variance explained by components.
        - Draws a horizontal line at 90% threshold to visualize 
          how many components are needed to reach 90% explained variance.
    """
    numeric_df = df.select_dtypes(include='number')
    
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(numeric_df)
    
    pca = PCA()
    pca.fit(scaled_data)

    cum_var = pca.explained_variance_ratio_.cumsum() * 100

    plt.figure(figsize=(8,5))
    plt.plot(range(1, len(cum_var)+1), cum_var)
    plt.axhline(90, color='r', linestyle='--', label='90% threshold')
    plt.xlabel("Number of components")
    plt.ylabel("Cumulative variance (%)")
    plt.title("PCA Cumulative Variance Curve")
    plt.grid(True)
    plt.legend()
    plt.show()


def main():
    """
    Load the dataset and plot the cumulative variance curve using PCA.
    """
    path = "../data/Train_knight.csv"
    
    df = pd.read_csv(path)

    plot_variance(df)


if __name__ == "__main__":
    main()

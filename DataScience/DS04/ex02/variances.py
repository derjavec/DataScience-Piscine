import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

def mean(data):
    return sum(data) / len(data)


def variance(data_sorted):
        n = len(data_sorted)
        m = mean(data_sorted)
        return sum((x - m)**2 for x in data_sorted)/n
def plot_variance(df):
    
    var = []
    for col in df.columns:
        data = pd.to_numeric(df[col], errors='coerce').dropna()
        if len(data) > 1:
            var.append(variance(data))

    total_var = sum(var)

    var.sort(reverse=True)

    var_cumulative = []
    var_sum = 0
    for v in var:
        var_sum += v
        var_cumulative.append(var_sum / total_var * 100)
    components = list(range(1, len(var_cumulative) + 1))

    plt.figure(figsize=(8,5))
    plt.plot(components, var_cumulative)
    plt.axhline(90, color='r', linestyle='--', label='90% threshold')
    plt.xlabel("Number of components")
    plt.ylabel("Cumulative variance (%)")
    plt.title("Cumulative variance curve")
    plt.grid(True)
    plt.legend()
    plt.show()

    
        

def main():
    """
    Load the dataset and plot the correlation heatmap.
    """
    path = "../data/Train_knight.csv"
    
    df = pd.read_csv(path)
    
    plot_variance(df)


if __name__ == "__main__":
    main()
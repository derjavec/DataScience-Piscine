import pandas as pd
from sklearn.preprocessing import LabelEncoder

def correlation(df):
    """
    Encode 'knight' as numbers and print correlation of all numeric columns with it.
    """
    encoder = LabelEncoder()
    df['knight_num'] = encoder.fit_transform(df['knight'])
    df_numeric = df.select_dtypes(include='number')
    correlations = df_numeric.corr()
    target_corr = correlations['knight_num'].sort_values(ascending=False)
    print(target_corr)

def main():
    """
    Load the dataset and compute correlations.
    """
    csv_path = "../data/Train_knight.csv"
    df = pd.read_csv(csv_path)
    correlation(df)

if __name__ == "__main__":
    main()

import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant


def calculate_vif(df):
    print(df)
    X = add_constant(df)
    print(X)
    vif_data = pd.DataFrame()
    vif_data["feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    print(vif_data)
    return vif_data

def optimise_features(df):

    while(True):
        df_vif = calculate_vif(X)
        max_vif = vif_table["VIF"].max()
        if max_vif > 5:
            
        


def main():
    """
    Load the dataset and 
    """
    path = "../data/Train_knight.csv"
    
    df = pd.read_csv(path)
    X = df.select_dtypes(include=["number"]).copy()
    optimise_features(X)
    


if __name__ == "__main__":
    main()
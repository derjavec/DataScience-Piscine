import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant


def calculate_vif(df):
    X = add_constant(df)
    vif_data = pd.DataFrame()
    vif_data["feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    return vif_data

def optimise_features(df):

    while(True):
        df_vif = calculate_vif(df)
        df_vif["Tolerance"] = 1 / df_vif["VIF"]
        df_vif_nc = df_vif[df_vif["feature"] != "const"]
        max_vif = df_vif_nc["VIF"].max()
        if max_vif > 5:
            max_feature = df_vif_nc.loc[df_vif_nc["VIF"].idxmax(), "feature"]
            print(max_feature)
            if max_feature in df.columns:
                df = df.drop(columns = max_feature)
        else:
            break
   
    print(df_vif_nc) 


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
import pandas as pd


def split(df, train_frac):
    df_shuffled = df.sample(frac=1, random_state=42)
    train_size = int(len(df_shuffled) * train_frac) 
    df_train = df_shuffled.iloc[:train_size]
    df_val = df_shuffled.iloc[train_size:]
    df_train.to_csv("Training_knight.csv", index=False)
    df_val.to_csv("Validation_knight.csv", index=False)

def main():
    csv_path = "../data/Train_knight.csv"
    df = pd.read_csv(csv_path)
    split(df, 0.8)

if __name__ == "__main__":
    main()
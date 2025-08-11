import pandas


def load(path: str):
    try:
        csv = pandas.read_csv(path)
        print(f"Loading dataset of dimensions {csv.shape}")
        return csv
    except Exception:
        return None


import matplotlib.pyplot as plt
import seaborn as sns
from load_csv import load


def clean_value(val):
    val = str(val).strip()
    if val.endswith("k"):
        return float(val[:-1]) * 1e3
    else:
        try:
            return float(val)
        except Exception:
            return None


def main():
    lifeExpCsv = load("life_expectancy_years.csv")
    gdaCsv = load("income_per_person_gdppercapita_ppp_inflation_adjusted.csv")

    if lifeExpCsv is None or gdaCsv is None:
        print("Error loading dataset.")
        return

    year = "1900"
    try:
        lifeExpByYear = lifeExpCsv[year].apply(clean_value)
        gdaByYear = gdaCsv[year].apply(clean_value)
    except Exception as e:
        print(f"No data for year {e}")
        return
    if lifeExpByYear.empty or gdaByYear.empty:
        print("No data found for one of the files")
        return

    plt.figure(figsize=(12, 6))
    plt.xscale("log")
    sns.scatterplot(x=gdaByYear, y=lifeExpByYear)
    plt.title
    (f"Correlation between gdp per capita and life expentancy in {year}")
    plt.xlabel("Gross domestic product")
    plt.ylabel("Life Expentacy")
    plt.xticks()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()

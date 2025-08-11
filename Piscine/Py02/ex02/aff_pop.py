import matplotlib.pyplot as plt
import seaborn as sns
from load_csv import load


def clean_population_values(values):
    cleaned = []
    for val in values:
        val = str(val).strip()
        if val.endswith("M"):
            cleaned.append(float(val[:-1]) * 1e6)
        elif val.endswith("B"):
            cleaned.append(float(val[:-1]) * 1e9)
        elif val.endswith("k"):
            cleaned.append(float(val[:-1]) * 1e3)
        else:
            cleaned.append(float(val))
    return cleaned


def main():
    totalPopulationCsv = load("population_total.csv")
    if totalPopulationCsv is None:
        print("Error loading dataset.")
        return

    country1 = "France"
    country2 = "Argentina"

    frPopulation = totalPopulationCsv
    [totalPopulationCsv["country"] == country1]
    arPopulation = totalPopulationCsv
    [totalPopulationCsv["country"] == country2]
    if frPopulation.empty or arPopulation.empty:
        if frPopulation.empty:
            country = country1
        else:
            country = country2
        print(f"No data found for country: {country}")
        return

    years = [int(year) for year in frPopulation.columns[1:]]
    frYearPopulation = clean_population_values(frPopulation.iloc[0, 1:].values)
    arYearPopulation = clean_population_values(arPopulation.iloc[0, 1:].values)

    plt.figure(figsize=(12, 6))
    sns.lineplot(x=years, y=frYearPopulation, label=country1)
    sns.lineplot(x=years, y=arYearPopulation, label=country2)
    plt.title(f"{country1} and {country2} Population ")
    plt.xlabel("Year")
    plt.ylabel("Population (years)")
    plt.xticks(years[::50])
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()

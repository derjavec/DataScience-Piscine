def ft_statistics(*args: any, **kwargs: any) -> None:
    data = []
    for x in args:
        try:
            num = float(x)
            data.append(num)
        except Exception:
            pass
    if len(data) == 0 and len(kwargs) > 0:
        print("Error: No arguments")
        return

    data_sorted = sorted(data)

    def mean():
        return sum(data) / len(data)

    def median():
        n = len(data_sorted)
        mid = n // 2
        if n % 2 == 0:
            return (data_sorted[mid - 1] + data_sorted[mid]) / 2
        else:
            return data_sorted[mid]

    def quartile():
        n = len(data_sorted)
        q1 = n // 4
        q3 = 3 * q1
        return [data_sorted[q1], data_sorted[q3]]

    def variance():
        n = len(data_sorted)
        m = mean()
        return sum((x - m)**2 for x in data_sorted)/n

    def std_dev():
        v = variance()
        return v ** 0.5

    funcs = {
        "mean": mean,
        "median": median,
        "quartile": quartile,
        "var": variance,
        "std": std_dev
    }

    for key in kwargs:
        if kwargs[key] in funcs:
            try:
                result = funcs[kwargs[key]]()
                print(f"{kwargs[key]} : {result}")
            except Exception:
                print(f"Error in function {kwargs[key]}")
        else:
            print(f"Function {kwargs[key]} does not exists")

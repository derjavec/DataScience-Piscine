def ft_tqdm(lst: range):
    total = len(lst)
    for i, elem in enumerate(lst, 1):
        percent = int(i / total * 100)
        bar = "=" * percent + " " * (100 - percent)
        print(f"\r{percent:3}%|[{bar}]| {i}/{total}", end="", flush=True)
        yield elem

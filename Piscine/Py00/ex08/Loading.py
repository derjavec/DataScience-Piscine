def ft_tqdm(lst: range):
    """
    Custom implementation of a progress bar generator, similar to tqdm.

    Iterates over the given iterable while printing a dynamic progress bar
    to the terminal showing percentage completed and the current position.
    """
    total = len(lst)
    for i, elem in enumerate(lst, 1):
        percent = int(i / total * 100)
        bar = "=" * percent + " " * (100 - percent)
        print(f"\r{percent:3}%|[{bar}]| {i}/{total}", end="", flush=True)
        yield elem

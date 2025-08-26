def ft_filter(function, iterable):
    """
    ft_filter(function or None, iterable) --> list

    Return a list of items from iterable for which function(item) is True.
    If function is None, return items that are truthy.
    """
    if function is None:
        return [item for item in iterable if item]
    return [item for item in iterable if function(item)]

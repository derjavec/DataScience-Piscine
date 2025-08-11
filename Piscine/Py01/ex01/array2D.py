
def slice_me(family: list, start: int, end: int) -> list:
    """
    Slices a 2D list (list of lists) between the start and end indices,
    prints the original and new shapes, and returns the sliced portion.
    """

    if not isinstance(family, list):
        raise TypeError("Input must be a list")
    if not all(isinstance(f, list) for f in family):
        raise TypeError("All members of the input must be lists")
    if not all(len(f) == len(family[0]) for f in family):
        raise ValueError("All lists must be of the same length")

    print(f"My shape is : {len(family)} , {len(family[0])}")

    sliced = family[start:end]

    if len(sliced) == 0:
        print("My shape is : (0,0)")
    else:
        print(f"My shape is : {len(sliced)} , {len(sliced[0])}")
    return sliced

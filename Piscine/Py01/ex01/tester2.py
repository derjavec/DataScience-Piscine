from array2D import slice_me

test_cases = [
    ("Valid case", [[1, 2], [3, 4], [5, 6]], 0, 2),
    ("Input not a list", "this is not a list", 0, 2),
    ("Elements not lists", [1, 2, 3], 0, 2),
    ("Inner lists of different lengths", [[1, 2], [3, 4, 5]], 0, 2),
    ("Empty slice", [[1, 2], [3, 4]], 3, 5),
]

for description, family, start, end in test_cases:
    print(f"\n{description}:")
    try:
        result = slice_me(family, start, end)
        print("Result:", result)
    except Exception as e:
        print("Error:", e)

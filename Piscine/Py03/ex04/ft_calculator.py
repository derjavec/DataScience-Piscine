class calculator:
    """
    A simple calculator class to perform vector operations.
    Supports dot product, element-wise addition,
    and element-wise subtraction of two vectors.
    All methods are static and do not require instantiation.
    """

    def __init__(self, V1, V2):
        """
        Initialize the calculator with two vectors.

        """
        self.V1 = V1
        self.V2 = V2

    @staticmethod
    def dotproduct(V1: list[float], V2: list[float]) -> None:
        """
        Compute and print the dot product of two vectors.
        """
        result = sum(x * y for x, y in zip(V1, V2))
        print(f"Dot product is: {result}")

    @staticmethod
    def add_vec(V1: list[float], V2: list[float]) -> None:
        """
        Compute and print the element-wise sum of two vectors.
        """
        result = [x + y for x, y in zip(V1, V2)]
        formated = [f"{num:.1f}" for num in result]
        print(f"Add vector is: {formated}")

    @staticmethod
    def sous_vec(V1: list[float], V2: list[float]) -> None:
        """
        Compute and print the element-wise subtraction
        (V1 - V2) of two vectors.
        """
        result = [x - y for x, y in zip(V1, V2)]
        formated = [f"{num:.1f}" for num in result]
        print(f"Sous vector is: {formated}")

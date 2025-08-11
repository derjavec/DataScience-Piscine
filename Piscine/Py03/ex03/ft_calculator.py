
def Print(obj):
    print(obj)


class calculator:
    """
    A simple calculator class that operates on a vector (list of numbers).
    Supports addition, subtraction, multiplication, and division by a scalar.
    All operations modify the vector in place and print the result.
    """

    def __init__(self, vector):
        """
        Initialize the calculator with a given vector.

        """
        self.vector = vector

    def __add__(self, scalar) -> None:
        """
        Add a scalar to each element of the vector, update the vector in place,
        and print the resulting vector.
        """
        self.vector = [x + scalar for x in self.vector]
        print(self.vector)

    def __mul__(self, scalar) -> None:
        """
        Multiply each element of the vector by a scalar,
        update the vector in place,
        and print the resulting vector.
        """
        self.vector = [x * scalar for x in self.vector]
        print(self.vector)

    def __sub__(self, scalar) -> None:
        """
        Subtract a scalar from each element of the vector,
        update the vector in place,
        and print the resulting vector.
        """
        self.vector = [x - scalar for x in self.vector]
        print(self.vector)

    def __truediv__(self, scalar) -> None:
        """
        Divide each element of the vector by a scalar,
        update the vector in place, and print the resulting vector.
        If scalar is zero, print an error message and do nothing.
        """
        if scalar == 0:
            print("Error: Division by zero")
            return
        self.vector = [x / scalar for x in self.vector]
        print(self.vector)

import sys
from ft_filter import ft_filter


def filter_string(s: str, n: int):
    """
    Filter the words in a string based on their length.
    """
    words = s.split()
    result = list(ft_filter(lambda word: len(word) > n, words))
    return result


def main():
    """
    Main program that processes command-line
    arguments and prints the filtered words.
    If the arguments are invalid, prints an AssertionError message.
    """
    assert len(sys.argv) == 3, "please provide a string and a number"
    assert sys.argv[2].isdigit(), "second argument must be an int"
    s = sys.argv[1]
    n = int(sys.argv[2])

    result = filter_string(s, n)
    print(result)
    print(filter.__doc__)
    print(ft_filter.__doc__)


if __name__ == "__main__":
    main()

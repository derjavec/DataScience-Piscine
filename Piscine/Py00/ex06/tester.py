import sys
from ft_filter import ft_filter


def filter_string(s: str):
    """
    Filter the words in a string based on their length.
    """
    words = s.split()
    result = list(ft_filter(lambda word: len(word) > 4, words))
    return result


def main():
    """
    Main program that processes command-line
    arguments and prints the filtered words.
    If the arguments are invalid, prints an AssertionError message.
    """
    if len(sys.argv) != 2:
        print("AssertionError: please provide a string")
        return

    s = sys.argv[1]

    result = filter_string(s)
    print(result)


if __name__ == "__main__":
    main()

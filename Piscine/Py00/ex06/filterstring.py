import sys
from ft_filter import ft_filter

def filter_string(s: str, n: int):
    """
    Filter the words in a string based on their length.
    """
    words = s.split()
    result = [word for word in words if (lambda w: len(w) > n)(word)]
    return result


def main():
    """
    Main program that processes command-line arguments and prints the filtered words.
    If the arguments are invalid, prints an AssertionError message.
    """
    if len(sys.argv) != 3:
        print("AssertionError: please provide a string and a number")
        return
    
    s = sys.argv[1]

    try:
        n = int(sys.argv[2])
    except ValueError:
        print("AssertionError: second argument must be an int")
        return

    result = filter_string(s, n)
    print(result)

if __name__ == "__main__":
    main()

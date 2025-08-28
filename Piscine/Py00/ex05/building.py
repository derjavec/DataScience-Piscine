import sys


def countUpper(text: str):
    """Count and print the number of uppercase letters in the text."""
    count = sum(1 for c in text if c.isupper())
    print(f"{count} upper letters")


def countLower(text: str):
    """Count and print the number of lowercase letters in the text."""
    count = sum(1 for c in text if c.islower())
    print(f"{count} lower letters")


def countPunt(text: str):
    """
    Count and print the number of punctuation marks in the text.
    Uses a custom set of punctuation characters.
    """
    punctuation_chars = '''!"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~¿¡'''
    count = sum(1 for c in text if c in punctuation_chars)
    print(f"{count} punctuation marks")


def countSpace(text: str):
    """Count and print the number of whitespace characters in the text."""
    count = sum(1 for c in text if c.isspace())
    print(f"{count} spaces")


def countDigits(text: str):
    """Count and print the number of digit characters in the text."""
    count = sum(1 for c in text if c.isdigit())
    print(f"{count} digits")


def main():
    """
    Main function that receives a text argument or prompts the user for input,
    then prints the total number of characters and counts of different
    character types.
    """
    assert len(sys.argv) < 2, "more than one argument is provided"

    if len(sys.argv) == 2:
        arg = sys.argv[1]
    else:
        print("What is the text to count?")
        arg = sys.stdin.readline()

    characters = len(arg)
    print(f"The text contains {characters} characters")
    countUpper(arg)
    countLower(arg)
    countPunt(arg)
    countSpace(arg)
    countDigits(arg)


if __name__ == "__main__":
    main()

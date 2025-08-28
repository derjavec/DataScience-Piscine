import sys

MORSE = {
    " ": "/ ",
    "A": ".- ",    "B": "-... ",  "C": "-.-. ",  "D": "-.. ",
    "E": ". ",     "F": "..-. ",  "G": "--. ",   "H": ".... ",
    "I": ".. ",    "J": ".--- ",  "K": "-.- ",   "L": ".-.. ",
    "M": "-- ",    "N": "-. ",    "O": "--- ",   "P": ".--. ",
    "Q": "--.- ",  "R": ".-. ",   "S": "... ",   "T": "- ",
    "U": "..- ",   "V": "...- ",  "W": ".-- ",   "X": "-..- ",
    "Y": "-.-- ",  "Z": "--.. ",
    "0": "----- ", "1": ".---- ", "2": "..--- ", "3": "...-- ",
    "4": "....- ", "5": "..... ", "6": "-.... ", "7": "--... ",
    "8": "---.. ", "9": "----. ",
}


def morse_code(s: str):
    """
    Convert a given string into Morse code and print it.
    """
    s = s.upper()
    try:
        morse_list = [MORSE[c] for c in s]
    except KeyError as e:
        print(f"Character not supported: {e}")
        return

    print(''.join(morse_list))


def main():
    assert len(sys.argv) == 2, "please provide one string"

    morse_code(sys.argv[1])


if __name__ == "__main__":
    main()

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
    morse_list = []
    for c in s.upper():
        morse_list.append(MORSE[c])
    print(morse_list)


def main():
    """
    Main entry point of the program.
    
    Validates the command-line arguments and encodes the given string 
    into Morse code if valid.
    
    Rules:
        - Only one argument must be provided.
        - The argument must contain only alphanumerical characters.
    """
    if len(sys.argv) != 2:
        print("AssertionError: please provide one string")
        return
    
    s = sys.argv[1]
    if not s.isalnum():
        print("AssertionError: please provide only alphanumerical characters")
        return
    
    morse_code(s)


if __name__ == "__main__":
    main()

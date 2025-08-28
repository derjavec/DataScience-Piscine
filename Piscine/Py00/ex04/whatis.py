import sys


def whatis():
    if len(sys.argv) == 1:
        return
    assert len(sys.argv) <= 2, "more than one argument is provided"
    assert sys.argv[1].isdigit(), "argument is not an integer"
    arg = sys.argv[1]
    obj = int(arg)

    res = "Even" if obj % 2 == 0 else "Odd"
    print(f"I'm {res}")


if __name__ == "__main__":
    whatis()

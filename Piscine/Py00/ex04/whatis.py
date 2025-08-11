import sys


def whatis():
    if len(sys.argv) == 1:
        return
    elif len(sys.argv) > 2:
        print("AssertionError: more than one argument is provided")
        return

    arg = sys.argv[1]
    try:
        obj = int(arg)
    except Exception:
        print("AssertionError: argument is not an integer")
        return
    if isinstance(obj, int):
        if obj % 2 == 0:
            res = "Even"
        else:
            res = "Odd"
        print(f"i'm {res}")


if __name__ == "__main__":
    whatis()

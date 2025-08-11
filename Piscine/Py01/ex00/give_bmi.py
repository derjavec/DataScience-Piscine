import numpy


def give_bmi(height: list[int | float], weight: list[int | float])\
 -> list[int | float]:
    """
    Calculate the Body Mass Index (BMI) for each pair of height and weight.

    """
    if len(height) != len(weight):
        raise ValueError("Arrays must have the same length")
    if not all(isinstance(h, (int, float)) for h in height):
        raise ValueError("Every element of height array must be int or float")
    if not all(isinstance(w, (int, float)) for w in weight):
        raise ValueError("Every element of weight array must be int or float")

    h = numpy.array(height)
    w = numpy.array(weight)
    bmi = w / (h ** 2)
    return bmi.tolist()


def apply_limit(bmi: list[int | float], limit: int) -> list[bool]:
    """
    Apply a limit to the BMI values, returning True for BMIs above the limit.

    """
    if not all(isinstance(b, (int, float)) for b in bmi):
        raise ValueError("Every element of bmi array must be int or float")
    return [b > limit for b in bmi]

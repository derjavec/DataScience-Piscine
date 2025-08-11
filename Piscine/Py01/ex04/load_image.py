from PIL import Image
import numpy


def ft_load(path: str) -> numpy.array:

    try:
        img = Image.open(path)
    except Exception as e:
        print(f"error : {e}")
        return None

    img = img.convert('RGB')
    pixels = numpy.array(img)
    return pixels

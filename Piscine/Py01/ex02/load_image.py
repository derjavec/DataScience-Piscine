from PIL import Image
import numpy


def ft_load(path: str) -> numpy.array:

    try:
        img = Image.open(path)
    except Exception as e:
        print(f"error : {e}")
        return None

    print(f"The shape of image is: {img.size[::-1]+(3,)}")
    img = img.convert('RGB')
    pixels = numpy.array(img)
    return pixels

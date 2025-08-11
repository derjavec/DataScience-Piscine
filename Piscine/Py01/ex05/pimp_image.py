import matplotlib.pyplot as plt


def ft_invert(array):
    """
    Inverts the color of the image received.
    """
    inverted = []
    for row in array:
        new_row = []
        for pixel in row:
            r, g, b = pixel
            new_pixel = [255 - r, 255 - g, 255 - b]
            new_row.append(new_pixel)
        inverted.append(new_row)

    plt.imshow(inverted)
    plt.title("Inverted Image")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()
    return inverted


def ft_red(array):
    """
    Applies a red filter by keeping only the red channel and zeroing others.
    """
    red = []
    for row in array:
        new_row = []
        for pixel in row:
            r, g, b = pixel
            new_pixel = [r, 0, 0]
            new_row.append(new_pixel)
        red.append(new_row)

    plt.imshow(red)
    plt.title("Red Image")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()
    return red


def ft_green(array):
    """
    Applies a green filter by keeping only \
    the green channel and zeroing others.
    """
    green = []
    for row in array:
        new_row = []
        for pixel in row:
            r, g, b = pixel
            new_pixel = [0, g, 0]
            new_row.append(new_pixel)
        green.append(new_row)

    plt.imshow(green)
    plt.title("Green Image")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()
    return green


def ft_blue(array):
    """
    Applies a blue filter by keeping only the blue channel and zeroing others.
    """
    blue = []
    for row in array:
        new_row = []
        for pixel in row:
            r, g, b = pixel
            new_pixel = [0, 0, b]
            new_row.append(new_pixel)
        blue.append(new_row)

    plt.imshow(blue)
    plt.title("Blue Image")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()
    return blue


def ft_grey(array):
    """
    Converts the image to greyscale by averaging RGB values equally.
    """
    grey = []
    for row in array:
        new_row = []
        for pixel in row:
            r, g, b = pixel
            g = r // 3
            new_pixel = [g, g, g]
            new_row.append(new_pixel)
        grey.append(new_row)

    plt.imshow(grey)
    plt.title("Grey Image")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()
    return grey

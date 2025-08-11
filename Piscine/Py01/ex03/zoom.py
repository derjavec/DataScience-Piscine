import numpy as np
import matplotlib.pyplot as plt
from load_image import ft_load


def ft_zoom(image: np.ndarray) -> np.ndarray:
    """
    Crops the last 2/3 of the image in height and width,
    and selects only the first color channel (e.g. red).
    Prints the new shape and pixel values.

    """
    try:
        height, width = image.shape[:2]

        start_row = height // 3
        start_col = width // 3

        cropped = image[start_row:, start_col:, 0:1]

        print(f"New shape after slicing: {cropped.shape}")
        print(cropped)

        return cropped

    except Exception as e:
        print(f"Error cropping image: {e}")
        return None


def main():
    img = ft_load("animal.jpeg")
    if img is None:
        return

    zoomed = ft_zoom(img)
    if zoomed is None:
        return

    plt.imshow(zoomed, cmap='gray')
    plt.title("Zoomed Image (last 2/3)")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()


if __name__ == "__main__":
    main()

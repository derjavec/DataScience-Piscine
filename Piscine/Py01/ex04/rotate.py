import numpy as np
import matplotlib.pyplot as plt
from load_image import ft_load


def ft_zoom(image: np.ndarray) -> np.ndarray:
    """
    Crop the last 2/3 of the image (height and width) \
    and extract a single channel.
    Returns the cropped image as a numpy array with shape (h, w, 1).
    """
    try:
        h, w, _ = image.shape
        start_h = h // 3
        start_w = w // 3
        cropped = image[start_h:, start_w:, 0:1]
        print(f"The shape of image is: {cropped.shape}")
        print(cropped)
        return cropped
    except Exception as e:
        print(f"Error in zooming: {e}")
        return None


def transpose(matrix):
    rows = len(matrix)
    cols = len(matrix[0])
    transposed = []
    for c in range(cols):
        new_row = []
        for r in range(rows):
            new_row.append(matrix[r][c])
        transposed.append(new_row)
    return transposed


def ft_rotate(image: np.ndarray) -> np.ndarray:
    """
    Receives a cropped image and returns its transpose.
    Prints the new shape and content after transpose.
    """
    try:
        cropped_2d = image[:, :, 0]
        transposed = transpose(cropped_2d)
        transposed_np = np.array(transposed)
        print(f"New shape after Transpose: {transposed_np.shape}")
        print(transposed_np)
        return transposed_np
    except Exception as e:
        print(f"Error in rotating: {e}")
        return None


def main():
    img = ft_load("animal.jpeg")
    if img is None:
        return

    cropped_img = ft_zoom(img)
    if cropped_img is None:
        return

    rotated_img = ft_rotate(cropped_img)
    if rotated_img is None:
        return

    plt.imshow(rotated_img, cmap='gray')
    plt.title("Rotated Image")
    plt.xlabel("X axis")
    plt.ylabel("Y axis")
    plt.show()


if __name__ == "__main__":
    main()

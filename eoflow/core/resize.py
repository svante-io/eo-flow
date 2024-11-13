import numpy as np

# Taken from https://github.com/zzd1992/Numpy-Pytorch-Bicubic/tree/master


def resize_along_dim(im, dim, weights, field_of_view):
    tmp_im = np.swapaxes(im, dim, 1)
    if im.ndim == 2:
        tmp_out_im = np.sum(tmp_im[:, field_of_view] * weights, axis=-1)
    elif im.ndim == 3:
        weights = np.expand_dims(weights, -1)
        tmp_out_im = np.sum(tmp_im[:, field_of_view] * weights, axis=-2)

    return np.swapaxes(tmp_out_im, dim, 1)


def cubic(x):
    absx = np.abs(x)
    absx2 = absx**2
    absx3 = absx**3
    return (1.5 * absx3 - 2.5 * absx2 + 1) * (absx <= 1) + (
        -0.5 * absx3 + 2.5 * absx2 - 4 * absx + 2
    ) * ((1 < absx) & (absx <= 2))


def lanczos2(x):
    pi = np.pi
    return (
        (np.sin(pi * x) * np.sin(pi * x / 2) + np.finfo(np.float32).eps)
        / ((pi**2 * x**2 / 2) + np.finfo(np.float32).eps)
    ) * (abs(x) < 2)


def box(x):
    return ((-0.5 <= x) & (x < 0.5)) * 1.0


def linear(x):
    return (x + 1) * ((-1 <= x) & (x < 0)) + (1 - x) * ((0 <= x) & (x <= 1))


def kernel_info(name=None):
    method, kernel_width = {
        "bicubic": (cubic, 4.0),
        "lanczos": (lanczos2, 4.0),
        "nearest": (box, 1.0),
        "bilinear": (linear, 2.0),
        None: (cubic, 4.0),
    }.get(name)
    return method, kernel_width


def fix_scale_and_size(input_shape, output_shape, scale_factor):
    # First fixing the scale-factor (if given) to be standardized the function expects (a list of scale factors in the
    # same size as the number of input dimensions)
    if scale_factor is not None:
        # By default, if scale-factor is a scalar we assume 2d resizing and duplicate it.
        if np.isscalar(scale_factor):
            scale_factor = [scale_factor, scale_factor]

        # We extend the size of scale-factor list to the size of the input by assigning 1 to all the unspecified scales
        scale_factor = list(scale_factor)
        scale_factor.extend([1] * (len(input_shape) - len(scale_factor)))

    # Fixing output-shape (if given): extending it to the size of the input-shape, by assigning the original input-size
    # to all the unspecified dimensions
    if output_shape is not None:
        output_shape = list(np.uint(np.array(output_shape))) + list(
            input_shape[len(output_shape) :]  # noqa: E203
        )

    # Dealing with the case of non-give scale-factor, calculating according to output-shape. note that this is
    # sub-optimal, because there can be different scales to the same output-shape.
    if scale_factor is None:
        scale_factor = 1.0 * np.array(output_shape) / np.array(input_shape)

    # Dealing with missing output-shape. calculating according to scale-factor
    if output_shape is None:
        output_shape = np.uint(np.ceil(np.array(input_shape) * np.array(scale_factor)))

    return scale_factor, output_shape


def imresize(im, scale_factor=None, output_shape=None, kernel=None, antialiasing=True):
    scale_factor, output_shape = fix_scale_and_size(
        im.shape, output_shape, scale_factor
    )

    method, kernel_width = kernel_info(kernel)
    antialiasing *= scale_factor[0] < 1

    sorted_dims = np.argsort(np.array(scale_factor)).tolist()
    out_im = np.copy(im)
    for dim in sorted_dims:
        if scale_factor[dim] == 1.0:
            continue

        weights, field_of_view = contributions(
            im.shape[dim],
            output_shape[dim],
            scale_factor[dim],
            method,
            kernel_width,
            antialiasing,
        )
        out_im = resize_along_dim(out_im, dim, weights, field_of_view)

    return out_im


def contributions(in_length, out_length, scale, kernel, kernel_width, antialiasing):
    fixed_kernel = (lambda arg: scale * kernel(scale * arg)) if antialiasing else kernel
    kernel_width *= 1.0 / scale if antialiasing else 1.0

    out_coordinates = np.arange(1, out_length + 1)
    match_coordinates = 1.0 * out_coordinates / scale + 0.5 * (1 - 1.0 / scale)

    left_boundary = np.floor(match_coordinates - kernel_width / 2)
    expanded_kernel_width = np.ceil(kernel_width)

    field_of_view = np.expand_dims(left_boundary, axis=1) + np.arange(
        1, expanded_kernel_width + 1
    )
    field_of_view = field_of_view.astype("int64")

    weights = fixed_kernel(
        1.0 * np.expand_dims(match_coordinates, axis=1) - field_of_view
    )
    sum_weights = np.sum(weights, axis=1, keepdims=True)
    sum_weights[sum_weights == 0] = 1.0
    weights = np.divide(weights, sum_weights)
    field_of_view = np.clip(field_of_view - 1, 0, in_length - 1)

    return weights, field_of_view

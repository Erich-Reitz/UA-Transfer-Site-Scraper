def read_file(filename: str):
    """
    Reads a file and returns its contents as a string.

    :param filename: The name of the file to read.
    """
    with open(filename) as f:
        return f.read()

def strip_and_split_by_newlines(data: str):
    return data.strip().split("\n")


def strip_integers_split_by_commas(data: str):
    return [int(i) for i in data.strip().split(",")]

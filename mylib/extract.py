import requests


def extract(
    data="https://github.com/fivethirtyeight/data/raw/refs/heads/master/biopics/biopics.csv",
    file_path="data/biopics.csv",
):
    with requests.get(data) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
        return file_path

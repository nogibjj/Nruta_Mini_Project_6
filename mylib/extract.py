import requests


def extract(
    data="https://github.com/nruta-choudhari/Datasets/raw/refs/heads/main/biopics.csv",
    file_path="data/biopics.csv",
):
    with requests.get(data) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
        return file_path

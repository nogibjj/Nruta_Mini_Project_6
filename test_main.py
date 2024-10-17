import os
from mylib.extract import extract
from mylib.query import query
from mylib.transform_load import load


def test_extract():
    try:
        file_path = extract()
        assert file_path is not None, "Extracted file path is None"
        assert os.path.exists(file_path), f"{file_path} does not exist"
        print("test extract passed.")
    except Exception as e:
        print(f"test extract failed: {e}")


def test_load():
    try:
        result = load()
        assert (
            result == "db loaded or already loaded"
        ), "Load function did not return expected result"
    except Exception as e:
        print(f"test_load failed: {e}")


def test_query():
    try:
        result = query()
        assert (
            result == "query successful"
        ), "Query function did not return expected result"
    except Exception as e:
        print(f"test_query failed: {e}")


if __name__ == "__main__":
    test_extract()
    test_load()
    test_query()

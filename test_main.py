from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query


def test_extract():
    """Test extract"""
    test1 = extract()
    assert test1 is not None


def test_load():
    """Test load"""
    # Ensure the load function is returning the expected message
    expected_message = "Data loaded into split tables."
    test2 = load()
    assert test2 == expected_message


def test_query():
    """Test query"""
    # Ensure the query function returns a success message
    expected_message = "Query successful."
    test3 = query()
    assert test3 == expected_message


if __name__ == "__main__":
    test_extract()
    test_load()
    test_query()

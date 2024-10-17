from mylib.extract import extract
from mylib.query import query
from mylib.transform_load import load

# Extract
print("Extracting data....")
extract()

# Transform and Load
print("Transforming data...")
load()

# Query
print("Querying data...")
query()

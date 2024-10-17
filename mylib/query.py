"""Query the database"""

from dotenv import load_dotenv
from databricks import sql
import os

complex_query = """WITH year_released AS(
  SELECT title, avg(year_release) as year_released
  FROM default.nmc_biopics
  GROUP BY title
)

SELECT * FROM default.nmc_biopics
JOIN year_released
ON default.nmc_biopics.title = year_released.title
ORDER BY nmc_biopics.title ASC"""


def query():
    """Querying the database"""
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(complex_query)
            result = cursor.fetchall()

            for row in result:
                print(row)

            cursor.close()
            connection.close()
    return "query successful"


if __name__ == "__main__":
    query()

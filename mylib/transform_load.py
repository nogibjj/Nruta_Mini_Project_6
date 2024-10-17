import csv
import os
from dotenv import load_dotenv
from databricks import sql


def load(dataset="data/biopics.csv"):
    # Try to open the file with UTF-8 encoding first
    try:
        with open(dataset, newline="", encoding="utf-8") as csvfile:
            payload = csv.reader(csvfile, delimiter=",")
            next(payload)  # Skip the header
            process_data(payload)

    except UnicodeDecodeError as e:
        print(f"UTF-8 decoding failed: {e}")
        print("Retrying with ISO-8859-1 encoding...")

        # If utf-8 fails, try with ISO-8859-1
        try:
            with open(dataset, newline="", encoding="ISO-8859-1") as csvfile:
                payload = csv.reader(csvfile, delimiter=",")
                next(payload)  # Skip the header
                process_data(payload)

        except UnicodeDecodeError as e2:
            print(f"ISO-8859-1 decoding failed: {e2}")
            return "Failed to decode the file with both UTF-8 and ISO-8859-1"

    return "Data loaded successfully"


def process_data(payload):
    load_dotenv()

    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:
            # Create the table if it doesn't exist
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS nmc_biopics
                (title STRING, country STRING, year_release INT, box_office STRING, director STRING, 
                number_of_subjects INT, subject STRING, type_of_subject STRING, subject_race STRING, 
                subject_sex STRING, lead_actor_actress STRING);
                """
            )

            # Check if the table is already loaded
            cursor.execute("SELECT * FROM nmc_biopics")
            result = cursor.fetchall()

            if not result:
                print("Inserting data into the database...")
                string_sql = "INSERT INTO nmc_biopics VALUES"
                for i in payload:
                    string_sql += "\n" + str(tuple(i)) + ","
                string_sql = (
                    string_sql[:-1] + ";"
                )  # Remove the last comma and add a semicolon
                print(string_sql)

                cursor.execute(string_sql)

            cursor.close()
        connection.close()


if __name__ == "__main__":
    load()

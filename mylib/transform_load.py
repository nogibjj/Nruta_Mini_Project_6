import csv
import os
from dotenv import load_dotenv
from databricks import sql


def load(dataset="data/biopics.csv"):
    # Open the CSV file and load the data
    with open(dataset, newline="", encoding="ISO-8859-1") as file:
        payload = csv.reader(file, delimiter=",")
        next(payload)  # Skip header

        load_dotenv()  # Load environment variables

        with sql.connect(
            server_hostname=os.getenv("SERVER_HOSTNAME"),
            http_path=os.getenv("HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_KEY"),
        ) as connection:
            with connection.cursor() as cursor:
                # Create tables for biopics details and subjects
                # Table 1: Biopics details
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS nmc_biopics_details
                       (title STRING, country STRING, year_release INT,
                        box_office STRING, director STRING);
                    """
                )
                # Table 2: Biopics Subjects
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS nmc_biopics_subjects
                       (title STRING, number_of_subjects INT, subject STRING, 
                        type_of_subject STRING, subject_race STRING, 
                        subject_sex STRING, lead_actor_actress STRING);
                    """
                )

                # Prepare to insert all rows into the respective tables
                details_values = []
                subjects_values = []

                for row in payload:  # Load all rows without a limit
                    # Prepare values for nmc_biopics_details
                    details_values.append((row[0], row[1], int(row[2]), row[3], row[4]))

                    # Prepare values for nmc_biopics_subjects
                    subjects_values.append(
                        (
                            row[0],
                            int(row[5]),
                            row[6],
                            row[7],
                            row[8],
                            row[9],
                            row[10],
                        )
                    )

                # Insert into nmc_biopics_details
                cursor.executemany(
                    "INSERT INTO nmc_biopics_details VALUES (?, ?, ?, ?, ?)",
                    details_values,
                )

                # Insert into nmc_biopics_subjects
                cursor.executemany(
                    "INSERT INTO nmc_biopics_subjects VALUES (?, ?, ?, ?, ?, ?, ?)",
                    subjects_values,
                )

            connection.commit()  # Commit changes
            cursor.close()
            connection.close()

    return "Data loaded into split tables."


if __name__ == "__main__":
    load()

[![CI](https://github.com/nogibjj/Nruta_Mini_Project_6/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Nruta_Mini_Project_6/actions/workflows/cicd.yml)

# IDS 706 Week 6 Mini Project - Complex SQL Query for a MySQL Database

## 🏗️ Requirements
- Design a complex SQL query involving joins, aggregation, and sorting
- Provide an explanation for what the query is doing and the expected results

## 📂 Project Structure
```
.
├── Makefile
├── README.md
├── data
│   └── biopics.csv
├── main.py
├── mylib
│   ├── __init__.py
│   ├── extract.py
│   ├── query.py
│   └── transform_load.py
├── requirements.txt
├── setup.sh
└── test_main.py
```

## 📦 Files and Directories
- extract.py: Extracts raw data from the source.
- load.py: Loads processed data into tables.
- query.py: Executes complex SQL queries (self-joins, joins).
- test_main.py: Unit tests for project functions.

## 🛠️ Setup Instructions
1. Clone the repository:
```
git clone https://github.com/nogibjj/Nruta_Mini_Project_6
```

2. Navigate to the project directory:
```
cd Nruta_Mini_Project_6
```

3. Install the required dependencies:
```
pip install -r requirements.txt
```

## 📊 Dataset Description
The data for this project comes from the `biopics.csv` dataset provided by FiveThirtyEight. After importing the dataset, I split it into two tables to organize the information more effectively:

- `nmc_biopics_details`: This table contains core information about each biopic, such as the title, release year, and other relevant attributes.
- `nmc_biopics_subjects`: This table includes details about the subjects or individuals the biopics are based on, including the number of subjects featured and their respective roles.

## 🗃️ SQL Query
Once the data was split, I constructed the following query to analyze the average release year of the biopics alongside the subjects they feature:

```
SELECT 
    d.title, 
    AVG(d.year_release) AS year_released, 
    s.number_of_subjects, 
    s.subject
FROM default.nmc_biopics_details AS d
JOIN default.nmc_biopics_subjects AS s 
    ON d.title = s.title
GROUP BY 
    d.title, 
    s.number_of_subjects, 
    s.subject
ORDER BY d.title ASC;
```

Explanation:
- `SELECT`: Retrieves the title of each biopic, the average release year (year_released), the number of subjects, and the subjects' names.
- `JOIN`: Combines the nmc_biopics_details and nmc_biopics_subjects tables based on the biopic title.
- `GROUP BY`: Groups the results by biopic title, number of subjects, and the subjects themselves to ensure we get accurate averages and associations.
- `ORDER BY`: Sorts the output in ascending order by the biopic title for easier readability.

## 🎯 Expected Output
After running the SQL query, the output is as follows:
<img width="990" alt="Screenshot 2024-10-18 at 17 22 12" src="https://github.com/user-attachments/assets/7a4959f4-840b-47d4-af08-f7c234d0595d">

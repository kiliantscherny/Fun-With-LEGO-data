import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime
import csv
import os

# SOURCE_FILE_NAMES = [
#     "themes",
#     "colors",
#     "part_categories",
#     "parts",
#     "part_relationships",
#     "elements",
#     "sets",
#     "minifigs",
#     "inventories",
#     "inventory_parts",
#     "inventory_sets",
#     "inventory_minifigs",
# ]


# Preprocesses the CSV file to take care of rows with missing columns
def preprocess_csv(src_file):
    try:
        with open(src_file, "r", newline="", errors="ignore") as file:
            reader = csv.reader(file)
            header_row = next(reader)  # Read the header row
            expected_num_columns = len(header_row)  # Determine the number of columns based on the header

            # Add "inserted_at" to the header row
            header_row.append("inserted_at")
            cleaned_rows = [header_row]  # Add the updated header row to the cleaned rows

            # Iterate over each row in the CSV
            for row in reader:
                # Fill in missing columns with empty strings
                if len(row) < expected_num_columns:
                    row += [""] * (expected_num_columns - len(row))
                # Truncate extra columns
                elif len(row) > expected_num_columns:
                    row = row[:expected_num_columns]

                # Add the current timestamp to the row
                row.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                cleaned_rows.append(row)

        # Write the cleaned rows back to the original CSV file
        with open(src_file, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(cleaned_rows)
    except FileNotFoundError:
        logging.error(f"File not found: {src_file}")
        pass


# Function to convert CSV to Parquet
def format_to_parquet_callable(src_file_names):
    for src_file in src_file_names:
        if not src_file.endswith(".csv"):
            logging.error("Can only accept source files in CSV format, for the moment")
            continue

        # Preprocess the CSV file
        preprocess_csv(src_file)

        # Read the preprocessed CSV file into a PyArrow table
        table = pv.read_csv(src_file)

        # Write the PyArrow table to a Parquet file
        pq.write_table(table, src_file.replace(".csv", ".parquet"))

        # Delete the original csv file
        os.remove(src_file)


# # The tables from Rebrickable that we can use
# SOURCE_FILE_NAMES = [
#     "themes",
#     "colors",
#     "part_categories",
#     "parts",
#     "part_relationships",
#     "elements",
#     "sets",
#     "minifigs",
#     "inventories",
#     "inventory_parts",
#     "inventory_sets",
#     "inventory_minifigs",
# ]

# # The output CSV file names
# OUTPUT_CSV_FILE_NAMES = [
#         source_file
#         + "_"
#         + "2024-03-03"
#         + ".csv"
#         for source_file in SOURCE_FILE_NAMES
#     ]


# # print(OUTPUT_CSV_FILE_NAMES)

# format_to_parquet_callable(OUTPUT_CSV_FILE_NAMES)
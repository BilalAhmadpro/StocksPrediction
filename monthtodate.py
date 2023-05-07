import csv
import tempfile
import shutil

def swap_date_elements(csv_file):
    # Create a temporary file to write the modified CSV data
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)

    # Open the original CSV file for reading
    with open(csv_file, 'r') as file_in:
        reader = csv.reader(file_in)
        header = next(reader)  # Read the header row

        # Find the index of the "Date" column
        date_index = header.index('Date')

        # Open the temporary file for writing
        with open(temp_file.name, 'w', newline='') as file_out:
            writer = csv.writer(file_out)
            writer.writerow(header)  # Write the header row

            # Iterate over each row in the CSV file
            for row in reader:
                # Split the date into day, month, and year
                day, month, year = row[date_index].split('-')
                # Swap day and year
                modified_date = f"{year}-{month}-{day}"
                row[date_index] = modified_date
                writer.writerow(row)  # Write the modified row

    # Replace the original file with the modified file
    shutil.move(temp_file.name, csv_file)   
csv_file = 'AAPL.csv'  # Provide the path to your CSV file
swap_date_elements(csv_file)    
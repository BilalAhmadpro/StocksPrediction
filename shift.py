import csv
import tempfile
import shutil

def shift_columns(csv_file):
    # Create a temporary file to write the modified CSV data
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)

    # Open the original CSV file for reading
    with open(csv_file, 'r') as file_in:
        reader = csv.reader(file_in)
        header = next(reader)  # Read the header row

        # Get the index of the "date" column
        date_index = header.index('Date')

        # Rearrange the header row to move "date" to the first position
        header = ['Date'] + header[:date_index] + header[date_index+1:]

        # Open the temporary file for writing
        with open(temp_file.name, 'w', newline='') as file_out:
            writer = csv.writer(file_out)
            writer.writerow(header)  # Write the modified header

            # Iterate over each row in the CSV file
            for row in reader:
                # Rearrange the values in each row to match the new column order
                shifted_row = [row[date_index]] + row[:date_index] + row[date_index+1:]
                writer.writerow(shifted_row)  # Write the modified row

    # Replace the original file with the modified file
    shutil.move(temp_file.name, csv_file)
csv_file = 'AAPL.csv'  # Provide the path to your CSV file
shift_columns(csv_file)   
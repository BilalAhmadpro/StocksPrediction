import csv
import tempfile
import shutil

def remove_column(csv_file, column_name):
    # Create a temporary file to write the modified CSV data
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)

    # Open the original CSV file for reading
    with open(csv_file, 'r') as file_in:
        reader = csv.reader(file_in)
        header = next(reader)  # Read the header row

        # Check if the column exists
        if column_name not in header:
            print(f"Column '{column_name}' does not exist in the CSV file.")
            return

        # Find the index of the column to remove
        column_index = header.index(column_name)

        # Open the temporary file for writing
        with open(temp_file.name, 'w', newline='') as file_out:
            writer = csv.writer(file_out)

            # Write the modified header to the temporary file
            modified_header = [col for col in header if col != column_name]
            writer.writerow(modified_header)

            # Iterate over each row in the CSV file
            for row in reader:
                # Remove the specified column from the row
                modified_row = [col for i, col in enumerate(row) if i != column_index]
                # Write the modified row to the temporary file
                writer.writerow(modified_row)

    # Replace the original file with the modified file
    shutil.move(temp_file.name, csv_file)
csv_file = 'AAPL.csv'
column_name = 'Divident'
#column_name1 = 'Split'
remove_column(csv_file, column_name)   
#remove_column(csv_file, column_name1)  
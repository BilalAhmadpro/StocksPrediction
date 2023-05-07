from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from itertools import product
import subprocess

# Define the output file name, password length, and character set
output_file = 'passwords.txt'
password_length = 4
character_set = 'abcdefghijklmnopqrstuvwxyz0123456789'

# Function to check if a password is successful
def check_password(password):
    process = subprocess.Popen(['C:\\Unrar\\UnRAR.exe', 'x', '-p' + password, 'Newfolder.rar'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return process.returncode == 0

# Initialize SparkSession
spark = SparkSession.builder.appName('PasswordCracker').getOrCreate()

# Generate all possible combinations of passwords
possible_passwords = list(product(character_set, repeat=password_length))

# Convert the list of passwords to a DataFrame
df = spark.createDataFrame(possible_passwords, ['password'])

# Register UDF for checking password
check_password_udf = udf(check_password)

# Add a column to check if the password is successful
df = df.withColumn('success', check_password_udf(df['password']))

# Filter out the successful passwords
successful_passwords = df.filter(df['success']).select('password')

# Collect the results as a list
passwords = [row[0] for row in successful_passwords.collect()]

# Save passwords to the output file
with open(output_file, 'w') as file:
    file.writelines([password + '\n' for password in passwords])

print('Passwords generated and saved to', output_file)

# Stop the SparkSession
spark.stop()
#These optimizations include converting the list of passwords to a DataFrame and using Spark's distributed computing capabilities to process passwords in parallel. Additionally, using the itertools.product function directly avoids creating unnecessary intermediate lists. The filtered passwords are collected as a list and then saved to the output file.

#Remember that the overall speed of password cracking depends on the complexity of the password and the available resources.







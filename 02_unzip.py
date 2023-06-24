import os
import pandas as pd
import subprocess # for unzipping
import sqlite3 # for sql databases

# Specify the folder path containing the .dat and .chk files
input_path = 'Z:/A&A/_code/02_unzip'
output_path = 'Z:/A&A/_code/02_unzip/output/'

if not os.path.exists(output_path):
    # Create a new folder
    os.makedirs(output_path)

# Loop through all .dat files in the folder
for file in os.listdir(input_path):
    if file.endswith('.gz'):
        # Get the full file path for the .gz file
        gz_file_path = os.path.join(input_path, file)

        # Get the corresponding .dat and .chk file path
        chk_file_path = os.path.join(input_path, f"{os.path.splitext(file)[0]}.chk")
        dat_file_path = os.path.join(input_path, f"{os.path.splitext(file)[0]}.dat")

        # Unzip. Code provided by tjoynt. Check whether this works
        #subprocess.run(['C:/Program Files/PKWARE/PKZIPC/pkzipc.exe', '-extract', os.path.join(input_path, file)])

        # Verify file size and number of observations from .chk file
        with open(chk_file_path, 'r') as chk_file:
            chk_data = chk_file.readline().strip().split('|')
            expected_file_size = int(chk_data[0])
            expected_num_observations = int(chk_data[1])

        # Read the .dat file into a pandas DataFrame
        data = pd.read_csv(dat_file_path, sep='|')

        # Verify file size and number of observations
        actual_file_size = os.path.getsize(dat_file_path)
        actual_num_observations = len(data)

        # Compare expected vs actual file size and number of observations
        if actual_file_size == expected_file_size and actual_num_observations == expected_num_observations:
            # Save as SQLite database
            db_file = os.path.join(output_path, f"{os.path.splitext(file)[0]}.db")
            conn = sqlite3.connect(db_file)
            data.to_sql('data', conn, if_exists='replace', index=False)
            conn.close()
            print(f"File '{file}' converted to SQLite database successfully.")
        else:
            print(f"File '{file}' verification failed.")
            if actual_file_size != expected_file_size:
                print(f"File size didn't match. Actual File size: {actual_file_size}. Expected: {expected_file_size}")
            else:
                print(f"Number of Observations didn't match. Actual: {actual_num_observations}. Expected: {expected_num_observations}")
print("All .dat files processed successfully.")

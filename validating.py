from os import listdir
from os.path import isfile, join
import pandas as pd 
from datetime import date
import os

# Global variables
RAW_DIR = "RAW/in/"
OUTPUT_NAME = "houses"


# Get all files in a directory
all_files = sorted([f for f in listdir(f"{RAW_DIR}") if isfile(join(f"{RAW_DIR}", f))])
frames = []

# Import data
for filename in all_files:
    with open(f"{RAW_DIR}{filename}", "r") as file:
        data = file.read().splitlines()
        
    # Create DataFrame
    df = pd.DataFrame(data)
    
    
    # Get value for count validation
    given_count_data = (df.iloc[-1].item())
    # Clean value from symbol !
    given_count_data = given_count_data[1:]


    # Delete useless data that starts with symbol !
    df = df[~df[0].astype(str).str.startswith('!')]
    # Separate data by symbol |
    df = df[0].str.split('|',expand=True)
    # Set first row as columns
    df.columns = df.iloc[0]
    
    
    # Validate number or rows
    real_count_data = df.shape[0]
    if int(given_count_data) == real_count_data:
        print("Validation: Passed")
    else:
        print("Validation: Failed")
        print("Given count: ",given_count_data)
        print("Real count: ",real_count_data)
    
    # Delete first row from DataFrame 
    df = df[1:]
    # Reset index
    df = df.reset_index(drop=True)
    frames.append(df)

# Merged DataFrame
merged_df = pd.concat(frames, ignore_index=True)

merged_df
# column_names = list(merged_df.columns)
# print(column_names)


# Get today date
todays_date = date.today()
# Set the path to save
path = (f"RAW/{todays_date.year}/{todays_date.month}/{todays_date.day}")
# Check whether the specified path exists or not
isExist = os.path.exists(path)
if not isExist:
   # Create a new directory because it does not exist
   os.makedirs(path)

# Save merged DataFrame to CSV with ; separator without index column
merged_df.to_csv(f"{path}/{OUTPUT_NAME}.csv", sep=';', index=False)


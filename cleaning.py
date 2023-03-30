import os
import sys
import pandas as pd 
from datetime import date
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# Global variables
INPUT_NAME = "houses"
OUTPUT_NAME = "houses"


# Get today date
todays_date = date.today()
# Get the path to read input from
path = (f"RAW/{todays_date.year}/{todays_date.month}/{todays_date.day}/{INPUT_NAME}.csv")
# Check whether the specified file exists or not
isExist = os.path.isfile(path)
if not isExist:
    sys.exit("The file/directory doesn't exist")

# Read DataFrame from path
df = pd.read_csv(path, sep=';')


# Drop the columns which have only NA values
df.dropna(axis=1, how="all", inplace=True)
df = df.drop('DealCode', axis=1)
# Remove duplicated rows
df = df.drop_duplicates(keep='first')
# # Date Standardized 
date_cols = ["ActiveDate", "EndDate", "DateSold"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')

# Convert object to float and replace to delimiter
df['SalePrice'] = df['SalePrice'].str.replace(",",".").astype(float)


# Add default values for missing data
df.fillna({
    "ActiveDate": "1970-01-01",
    "EndDate": "1970-01-01",
    "MSSubClass": "0.0",
    "LotFrontage": "0.0",
    "Alley": "‘’",
    "MasVnrType": "0.0",
    "MasVnrArea": "0.0",
    "BsmtQual": "‘’",
    "BsmtCond": "‘’",
    "BsmtExposure": "‘’",
    "BsmtQual": "‘’",
    "BsmtFinType1": "‘’",
    "BsmtFinType2": "‘’",
    "Electrical": "‘’",
    "BsmtFullBath": "0.0",
    "HalfBath": "0.0",
    "FireplaceQu": "‘’",
    "GarageType": "‘’",
    "GarageYrBlt": "0.0",
    "GarageFinish": "‘’",
    "GarageQual": "‘’",
    "GarageCond": "‘’",
    "PoolQC": "‘’",
    "Fence": "‘’",
    "MiscFeature": "0"    
}, inplace=True)



# Convert string to lower case
def convert_to_lowercase(val):
    if type(val) == str:
        return val.lower()
    else:
        return val
    
df = df.applymap(convert_to_lowercase)


# Get today date
todays_date = date.today()
# Set the path to save
path = (f"BASE/{todays_date.year}/{todays_date.month}/{todays_date.day}")
# Check whether the specified path exists or not
isExist = os.path.exists(path)
if not isExist:
   # Create a new directory because it does not exist
   os.makedirs(path)

# Save merged DataFrame to CSV with ; separator without index column
df.to_csv(f"{path}/{OUTPUT_NAME}.csv", sep=';', index=False, na_rep='NULL')


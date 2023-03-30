#!/usr/bin/env python
# coding: utf-8

# In[23]:


from os import listdir
from os.path import isfile, join
import pandas as pd  
from datetime import datetime
import os
import sys


# In[34]:


BASE_DIR = "BASE"
INPUT_NAME = "houses"
OUTPUT_NAME = "prepated_data"


# In[35]:


# Get today date
todays_date = datetime.today()
# Get the path to read
path = (f"{BASE_DIR}/{todays_date.year}/{todays_date.month}/{todays_date.day}/{INPUT_NAME}.csv")


# In[36]:


# Check whether the specified file exists or not
isExist = os.path.isfile(path)
if not isExist:
    sys.exit("The file/directory doesn't exist")

# Read DataFrame from path
df = pd.read_csv(path, sep=';')


# In[37]:


# Scaling
df = df.replace(["ex", "gd", "ta", "fa", "po"], [1.0,0.8,0.5,0.2,0.0])
df = df.replace(["y", "p", "n"], [1.0,0.5,0.0])


# In[38]:


# Decomposition
df["DateSold"] = pd.to_datetime(df["DateSold"])
df["MonthSold"] = df["DateSold"].dt.month
df["YearSold"] = df["DateSold"].dt.year


# In[39]:


# Aggregation
df["TotalBsmtSF"] = df["BsmtFinSF1"] + df["BsmtFinSF2"] + df["BsmtUnfSF"]
df["TotalLivingArea"] = df["1stFlrSF"] + df["2ndFlrSF"] + df["LowQualFinSF"]

year_now = datetime.now().year
df["HouseAge"] = year_now - df["YearBuilt"]


# In[40]:


# Save prepared data to csv
path = f"PREPARED/{year_now}"
df.to_csv(f"{path}/{OUTPUT_NAME}_{year_now}.csv",index = False)


# In[41]:


df


# In[ ]:





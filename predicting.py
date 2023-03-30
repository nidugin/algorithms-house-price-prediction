import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from datetime import datetime


# Get today year
todays_date = datetime.today()
year = todays_date.year


# Get prepared data
input_path = f"PREPARED/{year}/prepated_data_{year}.csv"
df = pd.read_csv(input_path)


# Transform data into 2D
x = ((df["TotalLivingArea"] * 1.6) + (df["TotalBsmtSF"] * 0.6)) * (df["OverallQual"] * 3) * (df["OverallCond"] * 0.8)
y = df["SalePrice"]

# Split the data on 2 parts
x_train, x_test, y_train, y_test = train_test_split(x,y, test_size=0.2)

# Create linear regression object 
reg = LinearRegression()
reg.fit(x_train.values.reshape(-1,1), y_train)
y_pred = reg.predict(x_test.values.reshape(-1,1))

# Visualize the results 
plt.scatter(x_test, y_test, color="black")
plt.plot(x_test, y_pred, color="blue", linewidth=3)
plt.title("Residual Plot")
plt.xlabel("Predictor")
plt.ylabel("Response")
plt.show()

# Create result_df
result_df = pd.DataFrame()
result_df["HouseIndex"] = x_test.index
result_df["PredictedPrice"] = y_pred
result_df["SellPrice"] = y_test
result_df["PredictionError"] = abs(1 - (y_pred / y_test))

# Get mean prediction error
mean_prediction_error = result_df["PredictionError"].mean()
print(f"Prediction Error: {mean_prediction_error}")


# Save file
output_path = f"PROCESSED/{year}/House_Predicted.csv"
result_df.to_csv(output_path, index=False)


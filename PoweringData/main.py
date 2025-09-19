import pandas as pd
import json


## ---- Create function ETL ---- ##
def extract_tabular_data(file_path):
    """Extract data from a tabular file_format with pandas"""
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    elif file_path.endswith(".parquet"):
        return pd.read_parquet(file_path)
    else:
        raise Exception("Warning: Invalid file extension. Please try with .csv or .parquet!")

def extract_json_data(file_path):
    """Extract and flatten data from a csv file"""
    with open(file_path, "r") as file_json:
        raw_data = json.load(file_json)
    return pd.json_normalize(raw_data)

def transform_electricity_sales_data(raw_data: pd.DataFrame):
      """
    Transform electricity sales to find the total amount of electricity sold
    in the residential and transportation sectors.
    
    To transform the electricity sales data, you'll need to do the following:
    - Drop any records with NA values in the `price` column. Do this inplace.
    - Only keep records with a `sectorName` of "residential" or "transportation".
    - Create a `month` column using the first 4 characters of the values in `period`.
    - Create a `year` column using the last 2 characters of the values in `period`.
    - Return the transformed `DataFrame`, keeping only the columns `year`, `month`, `stateid`, `price` and `price-units`.
    """
      # Drop any records with NA values in the price column
      raw_data.dropna(subset=['price'], inplace=True)

      # Only keep records with a sectorName of "residential" or "transportation"
      cleaned_df = raw_data.loc[raw_data['sectorName'].isin(['residential', 'transportation']), :]

      # Create a month column using the first 4 characters of the values in period column
      cleaned_df['month'] = cleaned_df['period'].str[0:4]

      # Create a year column using the last 2 characters of the values in period column
      cleaned_df['year'] = cleaned_df['period'].str[5:]

      # keeping only the columns year, month, stateid, price and price-units
      cleaned_df = cleaned_df.loc[:, ['year', 'month', 'stateid', 'price', 'price-units']]

      return cleaned_df

def load(dataframe: pd.DataFrame, file_path: str):
    """Load a Dataframe to a file in either CSV or Parquet format"""
    if file_path.endswith('.csv'):
        dataframe.to_csv(file_path)
    elif file_path.endswith('.parquet'):
        dataframe.to_parquet(file_path)
    else: 
        raise Exception(f"Warning: {file_path} is not a valid file type. Please try again!")


## ---- Run Pipeline ---- ##

# Extract
try:
    raw_electricity_capability_df = extract_json_data("electricity_capability_nested.json")
    raw_electricity_sales_df = extract_tabular_data("electricity_sales.csv")

# Transform
    cleaned_electricity_sales_df = transform_electricity_sales_data(raw_electricity_sales_df)

# Load
    load(raw_electricity_capability_df, "loaded_capability.parquet")
    load(cleaned_electricity_sales_df, "loaded_electricity_sales.csv")
except Exception as e:
    print(f"someting went wrong: {e}")
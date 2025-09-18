import pandas as pd
import logging
import os

data_source = (r"C:\Users\guymn\Desktop\ETL&ELT\dataset\Netflix.csv")
# -----Logging----- #
logging.basicConfig(
    filename="netfilx_etl.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting ETL Pipeline")

# -----ETL----- #
def extract_csv(file_path):
    logging.info(f"Extracting data from csv {file_path}.")
    try:
        df = pd.read_csv(file_path)
        print("Loading files successfully")
        return df
    except FileNotFoundError as ke:
        logging.error(f"Cannot read {file_path}")
        print(f"Cannot read files error {ke}")
        return None

def transform(df):
    logging.info(f"Transforming dataframe from csv")

    if "country" in df.columns:
    #cleaning country column
        logging.debug(f"Cleaning 'country' column")
        df["country"] = df["country"].fillna(df["country"].mode()[0])
        df["country"] = df["country"].astype(str)
        df["country"] = df["country"].apply(lambda x : x.split(", ")[0])
    else:
        logging.warning("'country' column not found in dataframe")

    if "director" in df.columns and "cast" in df.columns:
    #cleaning director and cast column
        logging.debug(f"Cleaning 'director' and 'cast' column")
        df["director"].fillna("Unknow", inplace=True)
        df["cast"].fillna("Unknow", inplace=True)
    else:
        logging.warning("'director' and 'cast' column not found in dataframe")
    
    #drop column
    logging.debug(f"Dropping 'show_id' and 'description' column")
    df.drop(columns=["show_id", "description"], inplace=True, errors="ignore")

    #convert to datetime
    logging.debug(f"Convert datatype 'date_added' to datetime")
    df["date_added"] = pd.to_datetime(df["date_added"])
    return df

def loadto_csv (clean_data, file_name):
    logging.info(f"Loading {clean_data} file name : {file_name}")
    if os.path.exists(file_name):
        logging.warning(f"File {file_name} already exists. Overwriting...")
    clean_data.to_csv(file_name, index=False)
    logging.info(f"File {file_name} saved successfully")


# ----- Run Pipelinne----- #
extract_data = extract_csv(data_source)
transform_data = transform(extract_data)
loadto_csv(transform_data, "cleaned_netflix.csv")
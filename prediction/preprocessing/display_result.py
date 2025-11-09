import pandas as pd

df = pd.read_parquet("../datasets/data_out/preprocessed_data_train")

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

print(df.head())

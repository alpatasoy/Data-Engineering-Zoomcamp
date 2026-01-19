import sys

import pandas as pd

print("Pipeline module loaded successfully.")

print('arguments', sys.argv)

month = int(sys.argv[1])

print(f"pipeline for month: {month}")

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

# binary format of data, optimized for performance
df.to_parquet(f"output_{month}.parquet")
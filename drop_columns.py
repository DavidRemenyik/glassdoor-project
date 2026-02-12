import pandas as pd
import sys

input_file = sys.argv[1]

df = pd.read_csv(input_file)
df.drop(columns=["index", "advice"], inplace=True, errors='ignore')
df.to_csv("cleaned_file.csv", index=False)

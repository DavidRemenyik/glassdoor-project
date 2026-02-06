import pandas as pd

df = pd.read_csv("name_extraction.csv")
df.drop(columns=["index", "advice"], inplace=True, errors='ignore')
df.to_csv("cleaned_file.csv", index=False)

import pandas as pd

df = pd.read_csv("all_reviews.csv")

def extract_company(url):  
    marker = "Reviews/"
    start = url.find(marker)
    
    start += len(marker)
    end = url.find("-Reviews", start)
    
    return url[start:end]

df["company"] = df["firm_link"].apply(extract_company)
df.to_csv("name_extraction.csv", index=False)

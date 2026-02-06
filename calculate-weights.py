#!/usr/bin/env python3
import pandas as pd
import re
import sys

input_file = sys.argv[1]

categories = {
    "work_life_balance": r"work life balance|flexibility|flexible work|flexible hours",
    "culture_values": r"culture|values|ethic|environment",
    "diversity_inclusion": r"diversity|inclusion|equality",
    "career_opp": r"progression|career|training|promotion",
    "comp_benefits": r"salary|pay|benefits|compensation",
    "senior_mgmt": r"senior management|management|manager|leadership",
}

weights = {category: 0 for category in categories}

# Broke the data into chunks to avoid memory overflow
chunksize = 50000

for chunk in pd.read_csv(input_file, chunksize=chunksize, engine="python", on_bad_lines="skip"):
    for category, pattern in categories.items():
        pros = chunk["pros"].str.contains(pattern, regex=True).sum()
        cons = chunk["cons"].str.contains(pattern, regex=True).sum()
        
        weights[category] += pros - cons

for category, value in weights.items():
    print(f"{category}: {value}")

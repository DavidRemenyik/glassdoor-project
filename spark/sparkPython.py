from pyspark import SparkContext
import re

import os

# Point this to your Java 17 installation path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Initialize Spark
sc = SparkContext("local[*]", "ReviewWeightAnalysis")

# Define your categories and patterns from your original script
categories = {
    "work_life_balance": r"work life balance|flexibility|flexible work|flexible hours",
    "culture_values": r"culture|values|ethic|environment",
    "diversity_inclusion": r"diversity|inclusion|equality",
    "career_opp": r"progression|career|training|promotion",
    "comp_benefits": r"salary|pay|benefits|compensation",
    "senior_mgmt": r"senior management|management|manager|leadership",
}

# 1. Load Data
# Assuming CSV format with 'pros' and 'cons' columns
raw_data = sc.textFile("glassdoor_reviews.csv")
header = raw_data.first()
rows = raw_data.filter(lambda line: line != header).map(lambda l: l.split(","))

# 2. MAP PHASE: Processes each row individually
def map_weights(row):
    # Map the relevant indices (update these based on your actual CSV structure)
    try:
        pros_text = row[10].lower() # Example index for 'pros'
        cons_text = row[11].lower() # Example index for 'cons'
        
        local_weights = []
        for category, pattern in categories.items():
            # Check if pattern exists in pros (+1) or cons (-1)
            p_match = 1 if re.search(pattern, pros_text) else 0
            c_match = 1 if re.search(pattern, cons_text) else 0
            local_weights.append((category, p_match - c_match))
        return local_weights
    except:
        return []

# Flatten the list of tuples so we have (category, score) pairs
mapped_rdd = rows.flatMap(map_weights)

# 3. REDUCE PHASE: Sums the scores for each category
def reduce_weights(a, b):
    return a + b

final_weights = mapped_rdd.reduceByKey(reduce_weights)

# Display Results
for category, score in final_weights.collect():
    print(f"{category}: {score}")

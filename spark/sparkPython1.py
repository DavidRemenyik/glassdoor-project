import sys
import re
import csv
from pyspark import SparkContext

def main():
    # Initialize Spark Context (Spark Core API)
    sc = SparkContext(appName="GlassdoorWeightMapReduce")
    
    # Check for input file argument
    if len(sys.argv) < 2:
        print("Usage: spark-submit spark_mapreduce.py <input_file>")
        sys.exit(1)
        
    input_path = sys.argv[1]

    # Patterns from your original logic
    categories = {
        "work_life_balance": r"work life balance|flexibility|flexible work|flexible hours",
        "culture_values": r"culture|values|ethic|environment",
        "career_opp": r"progression|career|training|promotion",
        "comp_benefits": r"salary|pay|benefits|compensation",
        "senior_mgmt": r"senior management|management|manager|leadership",
    }

    # 1. Load the raw text file
    raw_rdd = sc.textFile(input_path)

    # 2. MAP PHASE: Processes each line into category scores
    def map_sentiment(line):
        try:
            # Use csv reader to handle commas inside quotes correctly
            reader = csv.reader([line])
            row = next(reader)
            
            # Since there is no header, we use indices: 10=pros, 11=cons
            pros_text = row[10].lower()
            cons_text = row[11].lower()
            
            results = []
            for cat, pattern in categories.items():
                # Count if pattern exists in pros (+1) or cons (-1)
                p_score = 1 if re.search(pattern, pros_text) else 0
                c_score = 1 if re.search(pattern, cons_text) else 0
                # Emit (category, score)
                results.append((cat, p_score - c_score))
            return results
        except:
            # Handle potential bad lines/empty rows
            return []

    # 3. REDUCE PHASE: Aggregate the scores globally
    # flatMap turns the list of tuples into individual stream entries
    # reduceByKey sums the integers for each string key
    final_counts = raw_rdd.flatMap(map_sentiment) \
                          .reduceByKey(lambda a, b: a + b)

    # 4. OUTPUT
    output = final_counts.collect()
    print("\n--- MapReduce Weight Results ---")
    for category, score in output:
        print(f"{category}: {score}")

if __name__ == "__main__":
    main()

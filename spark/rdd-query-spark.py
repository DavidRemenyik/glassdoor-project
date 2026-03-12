import sys
from pyspark import SparkContext, SparkConf
import os


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"


def parse_and_score(line):
    # Split the CSV line
    cols = line.split(',')
    
    # Ensure we have enough columns to avoid index errors on malformed rows
    if len(cols) < 11:
        return ("FILTER_ME", (0, 0.0))
        
    firm = cols[0]
    
    # Extract the rating columns (Indices 6 to 10 based on your SQL schema)
    wlb_str = cols[6]
    cv_str = cols[7]
    co_str = cols[8]
    cb_str = cols[9]
    sm_str = cols[10]
    
    # Filter out rows with NULLs in the required columns
    if "NULL" in [wlb_str, cv_str, co_str, cb_str, sm_str]:
        return ("FILTER_ME", (0, 0.0))
    
    try:
        wlb = float(wlb_str)
        cv = float(cv_str)
        co = float(co_str)
        cb = float(cb_str)
        sm = float(sm_str)
    except ValueError:
        # Catch any unexpected string-to-float conversion errors
        return ("FILTER_ME", (0, 0.0))
    
    # Calculate the weighted score per your team's specific formula
    weighted_score = (
        (wlb * 1.1197) + 
        (cv * 1.4247) + 
        (co + (co * -0.0129)) + 
        (cb * 1.2584) + 
        (sm + (sm * -0.3348))
    ) / 5.0
    
    # Output Key-Value Pair: (Firm, (Count, Score))
    return (firm, (1, weighted_score))

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 rdd-query-spark.py <path_to_csv>")
        sys.exit(1)
        
    input_file = sys.argv[1]

    # Initialize SparkContext
    conf = SparkConf().setAppName("GlassdoorRDDMapReduce")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Load the text file
    lines = sc.textFile(input_file)

    # Extract and filter out the header row
    header = lines.first()
    data_lines = lines.filter(lambda line: line != header)

    # ==========================================
    # 1. MAP PHASE (Narrow Dependency)
    # ==========================================
    # Apply parsing function and remove invalid rows
    mapped_rdd = data_lines.map(parse_and_score).filter(lambda x: x[0] != "FILTER_ME")

    # ==========================================
    # 2. REDUCE PHASE (Wide Dependency)
    # ==========================================
    # Aggregate counts and scores by Firm (Key)
    reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # ==========================================
    # 3. POST-PROCESSING & FILTERING
    # ==========================================
    # Keep only firms with >= 1000 reviews
    filtered_firms = reduced_rdd.filter(lambda x: x[1][0] >= 1000)

    # Calculate final average: Total Score / Count
    final_averages = filtered_firms.mapValues(lambda v: v[1] / v[0])

    # ==========================================
    # 4. ACTION
    # ==========================================
    # Sort by the average rating in descending order
    sorted_rdd = final_averages.sortBy(lambda x: x[1], ascending=False)

    # Take the top 10 results back to the driver
    top_10 = sorted_rdd.take(10)

    # Print results in a clean format to mimic your old output
    print("\n--- TOP 10 WEIGHTED COMPANIES (RDD CORE API) ---")
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+")
    print(f"| {'firm':<28} | {'review_count':<12} | {'weighted_rating':<18} |")
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+")
    
    # Note: We need to pull the original counts back to print them nicely. 
    # Since we mapped values to just the average above, let's grab the counts from the filtered_firms RDD
    # A cleaner way is to keep the count in the mapValues step:
    final_with_counts = filtered_firms.mapValues(lambda v: (v[0], v[1] / v[0]))
    sorted_with_counts = final_with_counts.sortBy(lambda x: x[1][1], ascending=False)
    top_10_final = sorted_with_counts.take(10)
    
    for firm, (count, avg_rating) in top_10_final:
        # Truncate firm name if it's too long for the table
        display_firm = firm[:28] if len(firm) > 28 else firm
        print(f"| {display_firm:<28} | {count:<12} | {avg_rating:<18.15f} |")
        
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+\n")

    sc.stop()

if __name__ == "__main__":
    main()

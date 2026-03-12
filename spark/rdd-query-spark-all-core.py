import sys, os
from pyspark import SparkContext, SparkConf
print("Total CPU Cores available:", os.cpu_count())
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# To see Spark's default parallelism (usually matches core count in local mode)
def parse_partition(iterator):
    for line in iterator:
        cols = line.split(',')
        
        # Ensure the row has enough columns
        if len(cols) < 11:
            continue
            
        firm = cols[0]
        
        # Check ONLY our 5 specific rating columns for "NULL"
        if "NULL" in [cols[6], cols[7], cols[8], cols[9], cols[10]]:
            continue
        
        try:
            wlb = float(cols[6])
            cv = float(cols[7])
            co = float(cols[8])
            cb = float(cols[9])
            sm = float(cols[10])
        except ValueError:
            # Catch any unexpected string-to-float errors
            continue
        
        # Calculate the weighted score per your team's formula
        weighted_score = (
            (wlb * 1.1197) + 
            (cv * 1.4247) + 
            (co + (co * -0.0129)) + 
            (cb * 1.2584) + 
            (sm + (sm * -0.3348))
        ) / 5.0
        
        # 'yield' makes this a generator, returning one pair at a time efficiently
        yield (firm, (1, weighted_score))

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 rdd-query-spark.py <path_to_csv>")
        sys.exit(1)
        
    input_file = sys.argv[1]

    # OPTIMIZATION: Tell Spark to use ALL laptop cores (local[*]) 
    # and use the ultra-fast Kryo Serializer for the reduce phase
    conf = SparkConf().setAppName("GlassdoorRDDMapReduce") \
                      .setMaster("local[*]") \
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Load the text file
    lines = sc.textFile(input_file)

    print("Spark Parallelism:", sc.defaultParallelism)
    # Extract and filter out the header row
    header = lines.first()
    data_lines = lines.filter(lambda line: line != header)

    # ==========================================
    # 1. MAP PHASE (Narrow Dependency)
    # ==========================================
    # Use mapPartitions to pass whole chunks of data into our function at once
    mapped_rdd = data_lines.mapPartitions(parse_partition)

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

    # Calculate final average while preserving the count
    final_with_counts = filtered_firms.mapValues(lambda v: (v[0], v[1] / v[0]))

    # ==========================================
    # 4. ACTION
    # ==========================================
    # Sort by the average rating in descending order
    sorted_with_counts = final_with_counts.sortBy(lambda x: x[1][1], ascending=False)

    # Take the top 10 results back to the driver
    top_10_final = sorted_with_counts.take(10)

    # Print results in a clean format
    print("\n--- TOP 10 WEIGHTED COMPANIES (OPTIMIZED RDD CORE API) ---")
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+")
    print(f"| {'firm':<28} | {'review_count':<12} | {'weighted_rating':<18} |")
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+")
    
    for firm, (count, avg_rating) in top_10_final:
        display_firm = firm[:28] if len(firm) > 28 else firm
        print(f"| {display_firm:<28} | {count:<12} | {avg_rating:<18.15f} |")
        
    print(f"+{'-'*30}+{'-'*14}+{'-'*20}+\n")

    sc.stop()

if __name__ == "__main__":
    main()

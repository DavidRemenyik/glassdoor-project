import csv
import sys

INPUT = sys.argv[1]
OUTPUT = "renamed_reordered.csv"

ORDER = [
    "firm",
    "date_review",
    "job_title",
    "current",
    "location",
    "overall_rating",
    "work_life_balance",
    "culture_values",
    "diversity_inclusion",
    "career_opp",
    "comp_benefits",
    "senior_mgmt",
    "recommend",
    "ceo_approv",
    "outlook",
    "headline",
    "pros",
    "cons"
]

with open(INPUT, newline='', encoding='utf-8') as fin, \
     open(OUTPUT, "w", newline='', encoding='utf-8') as fout:

    reader = csv.DictReader(fin)
    writer = csv.writer(fout)

    writer.writerow(ORDER)  # header

    for row in reader:
        # mapping "company" extracted name to "firm"
        row["firm"] = row.get("company", "")

        # Normalize missing location values
        if not row.get("location"):
            row["location"] = "NULL"

        writer.writerow([row.get(col, "") for col in ORDER])


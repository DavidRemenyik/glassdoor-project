import csv
import sys

INPUT = sys.argv[1]
OUTPUT = "renamed.csv"

# Mapping from NEW-DATASET → OLD-DATASET names
RENAME = {
    "rating": "overall_rating",
    "title": "headline",
    "status": "current",
    "pros": "pros",
    "cons": "cons",
    "Recommend": "recommend",
    "CEO Approval": "ceo_approv",
    "Business Outlook": "outlook",
    "Career Opportunities": "career_opp",
    "Compensation and Benefits": "comp_benefits",
    "Senior Management": "senior_mgmt",
    "Work/Life Balance": "work_life_balance",
    "Culture & Values": "culture_values",
    "Diversity & Inclusion": "diversity_inclusion",
    "firm_link": "firm",
    "date": "date_review",
    "job": "job_title"
}

with open(INPUT, newline='', encoding='utf-8') as fin, \
     open(OUTPUT, "w", newline='', encoding='utf-8') as fout:

    reader = csv.DictReader(fin)
    
    # Rename header but keep all columns
    new_fieldnames = [RENAME.get(col, col) for col in reader.fieldnames]

    writer = csv.DictWriter(fout, fieldnames=new_fieldnames)
    writer.writeheader()

    for row in reader:
        new_row = {RENAME.get(col, col): value for col, value in row.items()}
        writer.writerow(new_row)


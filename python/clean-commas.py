import csv
import sys

INPUT = sys.argv[1]
OUTPUT = "output.csv"

COLUMNS = {
    "firm": 0,
    "date_review": 1,
    "job_title": 2,
    "current": 3,
    "location": 4,
    "overall_rating": 5,
    "work_life_balance": 6,
    "culture_values": 7,
    "diversity_inclusion": 8,
    "career_opp": 9,
    "comp_benefits": 10,
    "senior_mgmt": 11,
    "recommend": 12,
    "ceo_approv": 13,
    "outlook": 14,
    "headline": 15,
    "pros": 16,
    "cons": 17
}

TEXT_FIELDS = [
    "firm", "date_review", "job_title", "current", "location",
    "headline", "pros", "cons"
]

NUMERIC_FIELDS = [
    "overall_rating",
    "work_life_balance",
    "culture_values",
    "diversity_inclusion",
    "career_opp",
    "comp_benefits",
    "senior_mgmt"
]

TEXT_COLS = [COLUMNS[key] for key in TEXT_FIELDS]
NUMERIC_COLS = [COLUMNS[key] for key in NUMERIC_FIELDS]

with open(INPUT, newline='', encoding='utf-8') as fin, \
     open(OUTPUT, 'w', newline='', encoding='utf-8') as fout:

    reader = csv.reader(fin)
    writer = csv.writer(fout, quoting=csv.QUOTE_MINIMAL)

    for row in reader:

        # Clean text fields
        for idx in TEXT_COLS:
            row[idx] = (
                row[idx]
                .replace(",", "")
                .replace("'", "")
                .replace("\t", " ")
                .replace("\n", " ")
                .replace("\r", " ")
                .strip()
            )

        # Convert numeric fields
        for idx in NUMERIC_COLS:
            try:
                row[idx] = str(float(row[idx]))
            except:
                row[idx] = "NULL"

        writer.writerow(row)


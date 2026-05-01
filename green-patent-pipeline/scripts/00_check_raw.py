# scripts/00_check_raw.py
"""
Verifies all required raw data files are present and have the
expected columns before running the pipeline.
Run this first: python3 scripts/00_check_raw.py
"""
import pandas as pd
import os
import sys

# Force working directory to project root
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"Working directory: {os.getcwd()}")

RAW = "data/raw"

REQUIRED_FILES = {
    "g_patent.tsv": [
        "patent_id", "patent_title", "patent_date",
        "patent_type", "withdrawn"
    ],
    "g_patent_abstract.tsv": [
        "patent_id", "patent_abstract"
    ],
    "g_inventor_disambiguated.tsv": [
        "patent_id", "inventor_id",
        "disambig_inventor_name_first",
        "disambig_inventor_name_last",
        "location_id"
    ],
    "g_assignee_disambiguated.tsv": [
        "patent_id", "assignee_id",
        "disambig_assignee_organization",
        "disambig_assignee_individual_name_first",
        "disambig_assignee_individual_name_last",
        "assignee_type", "location_id"
    ],
    "g_location_disambiguated.tsv": [
        "location_id", "disambig_country",
        "disambig_city", "disambig_state"
    ],
    "g_cpc_current.tsv": [
        "patent_id", "cpc_group"
    ]
}

print("\n" + "=" * 55)
print("CHECKING RAW DATA FILES")
print("=" * 55)

all_ok = True
for filename, expected_cols in REQUIRED_FILES.items():
    filepath = os.path.join(RAW, filename)
    if not os.path.exists(filepath):
        print(f"\n  ✗ MISSING: {filename}")
        all_ok = False
        continue

    # Read just 3 rows to check columns
    df = pd.read_csv(filepath, sep="\t", nrows=3,
                     low_memory=False, dtype=str)
    actual_cols = list(df.columns)
    missing_cols = [c for c in expected_cols if c not in actual_cols]
    size_mb = os.path.getsize(filepath) / (1024**2)

    if missing_cols:
        print(f"\n  ✗ {filename} ({size_mb:.0f} MB)")
        print(f"    Missing columns: {missing_cols}")
        all_ok = False
    else:
        print(f"\n  ✓ {filename} ({size_mb:.0f} MB)")
        print(f"    Columns OK: {expected_cols}")

print("\n" + "=" * 55)
if all_ok:
    print("ALL FILES OK — ready to run 02_clean.py")
else:
    print("FIX MISSING FILES BEFORE CONTINUING")
print("=" * 55)
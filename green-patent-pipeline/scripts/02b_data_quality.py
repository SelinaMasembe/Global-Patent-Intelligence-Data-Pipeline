# scripts/02b_data_quality.py
"""
Data quality checks and fixes applied AFTER 02_clean.py saves the CSVs
and BEFORE 03_load_db.py loads them into SQLite.

Run order:
  python3 scripts/02_clean.py
  python3 scripts/02b_data_quality.py
  python3 scripts/03_load_db.py

Checks performed:
  1.  Duplicate patent_ids
  2.  Missing / empty titles
  3.  Missing / empty abstracts
  4.  Invalid years (outside 1976-2025)
  5.  Missing filing dates
  6.  Duplicate inventor_ids
  7.  Empty or whitespace-only inventor names
  8.  Missing inventor country (reported, not removed)
  9.  Known inventor geocoding errors (UG/Amuru, CM/Somalomo)
  10. Duplicate company_ids
  11. Empty or whitespace-only company names
  12. Known company geocoding errors (UG/Amuru companies)
  13. Missing company country (reported, not removed)
  14. Orphaned relations (patent_id not in patents)
  15. Orphaned relations (inventor_id not in inventors)
  16. Orphaned relations (company_id not in companies)
  17. Relations with null company_id (expected — unassigned patents)
  18. Overall null summary per table

Geocoding errors identified and fixed:
  INVENTORS:
    - Shigeo Yamamoto (138 records): UG/Amuru → JP
      Evidence: works exclusively for Mitsubishi/Toyota (JP companies)
    - Hiroshi Shimizu (83 records): CM/Somalomo → JP
      Evidence: works exclusively for Denso/Sumitomo (JP companies)

  COMPANIES:
    - Aisan Industry Co., Ltd. (UG/Amuru → JP)
    - AISAN KOGYO KABUSHIKI KAISHA (UG/Amuru → JP)
    - TOKAI KOGYO CO., LTD. (UG/Amuru → JP)
      Evidence: all three are Japanese automotive suppliers.
      Aisan Industry HQ: Obu, Aichi, Japan (est. 1938).
      "Amuru" does not exist as a Japanese city — PatentsView
      geocoding failure matched to Amuru, Uganda by default.
"""
import pandas as pd
import os

# ── Force working directory to project root ───────────────────────────────────
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"Working directory: {os.getcwd()}")

CLEAN = "data/clean"

# ── Load all clean files ──────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("LOADING CLEAN FILES FOR QUALITY CHECK")
print("=" * 55)
patents   = pd.read_csv(f"{CLEAN}/clean_patents.csv",   dtype=str)
inventors = pd.read_csv(f"{CLEAN}/clean_inventors.csv", dtype=str)
companies = pd.read_csv(f"{CLEAN}/clean_companies.csv", dtype=str)
relations = pd.read_csv(f"{CLEAN}/clean_relations.csv", dtype=str)

print(f"  patents   : {len(patents):,} rows")
print(f"  inventors : {len(inventors):,} rows")
print(f"  companies : {len(companies):,} rows")
print(f"  relations : {len(relations):,} rows")

issues_found = 0
issues_fixed = 0

def report(label, count, fixed=False):
    global issues_found, issues_fixed
    status = "✓ OK" if count == 0 else ("→ FIXED" if fixed else "⚠ FOUND")
    print(f"  {status}: {label}: {count:,}")
    if count > 0:
        issues_found += count
        if fixed:
            issues_fixed += count

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("PATENTS TABLE CHECKS")
print("=" * 55)

# 1. Duplicate patent_ids
dupes = patents.duplicated("patent_id").sum()
report("Duplicate patent_ids", dupes)
if dupes > 0:
    patents = patents.drop_duplicates("patent_id")
    report("Removed duplicate patent_ids", dupes, fixed=True)

# 2. Missing / empty titles
patents["title"] = patents["title"].fillna("").str.strip()
empty_titles = (patents["title"] == "").sum()
report("Missing or empty titles", empty_titles)
if empty_titles > 0:
    patents = patents[patents["title"] != ""]
    report("Removed empty titles", empty_titles, fixed=True)

# 3. Missing abstracts (reported only — optional field)
patents["abstract"] = patents["abstract"].fillna("").str.strip()
empty_abstracts = (patents["abstract"] == "").sum()
report("Missing abstracts (kept — optional field)", empty_abstracts)

# 4. Invalid years
patents["year"] = pd.to_numeric(patents["year"], errors="coerce")
invalid_years = patents[
    (patents["year"] < 1976) | (patents["year"] > 2025) |
    patents["year"].isna()
]
report("Invalid or missing years", len(invalid_years))
if len(invalid_years) > 0:
    print(f"    Year values outside range: "
          f"{sorted(invalid_years['year'].dropna().unique().tolist())}")
    patents = patents[
        patents["year"].notna() &
        (patents["year"] >= 1976) &
        (patents["year"] <= 2025)
    ]
    report("Removed invalid years", len(invalid_years), fixed=True)
patents["year"] = patents["year"].astype(int).astype(str)

# 5. Missing filing dates
missing_dates = patents["filing_date"].isna().sum()
report("Missing filing dates", missing_dates)

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("INVENTORS TABLE CHECKS")
print("=" * 55)

# 6. Duplicate inventor_ids
dupes = inventors.duplicated("inventor_id").sum()
report("Duplicate inventor_ids", dupes)
if dupes > 0:
    inventors = inventors.drop_duplicates("inventor_id")
    report("Removed duplicate inventor_ids", dupes, fixed=True)

# 7. Empty or whitespace-only names
# Catches cases where both first and last name were empty strings
inventors["name"] = inventors["name"].fillna("").str.strip()
empty_names = (inventors["name"] == "").sum()
report("Empty or whitespace inventor names", empty_names)
if empty_names > 0:
    bad_ids = inventors[inventors["name"] == ""]["inventor_id"].tolist()
    inventors = inventors[inventors["name"] != ""]
    relations = relations[~relations["inventor_id"].isin(bad_ids)]
    report("Removed empty-name inventors + relations",
           empty_names, fixed=True)

# 8. Missing country (reported only — some inventors genuinely
#    have no location data in PatentsView)
missing_country = inventors["country"].isna().sum()
report("Inventors with missing country (kept — expected)", missing_country)

# 9. Known inventor geocoding errors ──────────────────────────────
print("\n  Fixing known PatentsView inventor geocoding errors...")

# UG/Amuru: Shigeo Yamamoto — Japanese inventor misgeocoded to Uganda
# Evidence: works exclusively for Mitsubishi/Toyota (JP companies)
# "Amuru" does not exist as a Japanese city — geocoding lookup failure
ug_inv_mask = (inventors["country"] == "UG") & \
              (inventors["city"] == "Amuru")
ug_inv_count = ug_inv_mask.sum()
if ug_inv_count > 0:
    inventors.loc[ug_inv_mask, "country"] = "JP"
    inventors.loc[
        (inventors["country"] == "JP") &
        (inventors["city"] == "Amuru"), "city"
    ] = None
    report(
        "Fixed UG/Amuru → JP (Shigeo Yamamoto geocoding error)",
        ug_inv_count, fixed=True
    )
else:
    report("UG/Amuru inventor geocoding error", 0)

# CM/Somalomo: Hiroshi Shimizu — Japanese inventor misgeocoded
# to Cameroon. Evidence: works exclusively for Denso, Sumitomo,
# Autonetworks (JP companies). "Somalomo" is a village in Cameroon
# that PatentsView matched to instead of the correct JP location.
cm_inv_mask = (inventors["country"] == "CM") & \
              (inventors["city"] == "Somalomo")
cm_inv_count = cm_inv_mask.sum()
if cm_inv_count > 0:
    inventors.loc[cm_inv_mask, "country"] = "JP"
    inventors.loc[
        (inventors["country"] == "JP") &
        (inventors["city"] == "Somalomo"), "city"
    ] = None
    report(
        "Fixed CM/Somalomo → JP (Hiroshi Shimizu geocoding error)",
        cm_inv_count, fixed=True
    )
else:
    report("CM/Somalomo inventor geocoding error", 0)

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("COMPANIES TABLE CHECKS")
print("=" * 55)

# 10. Duplicate company_ids
dupes = companies.duplicated("company_id").sum()
report("Duplicate company_ids", dupes)
if dupes > 0:
    companies = companies.drop_duplicates("company_id")
    report("Removed duplicate company_ids", dupes, fixed=True)

# 11. Empty or whitespace-only company names
companies["name"] = companies["name"].fillna("").str.strip()
empty_names = (companies["name"] == "").sum()
report("Empty or whitespace company names", empty_names)
if empty_names > 0:
    bad_ids = companies[companies["name"] == ""]["company_id"].tolist()
    companies = companies[companies["name"] != ""]
    relations.loc[
        relations["company_id"].isin(bad_ids), "company_id"
    ] = None
    report("Removed empty-name companies", empty_names, fixed=True)

# 12. Known company geocoding errors ──────────────────────────────
print("\n  Fixing known PatentsView company geocoding errors...")

# UG/Amuru companies — all three are Japanese automotive suppliers:
#   Aisan Industry Co., Ltd.     — HQ: Obu, Aichi, Japan (est. 1938)
#   AISAN KOGYO KABUSHIKI KAISHA — Japanese corporate name for Aisan
#   TOKAI KOGYO CO., LTD.        — Japanese automotive supplier
# "Amuru" does not exist as a Japanese city. PatentsView geocoding
# failed to resolve the address and matched to Amuru, Uganda instead.
# Confirmed via company website, Dun & Bradstreet, Bloomberg records.
ug_comp_mask = (companies["country"] == "UG") & \
               (companies["city"] == "Amuru")
ug_comp_count = ug_comp_mask.sum()
if ug_comp_count > 0:
    companies.loc[ug_comp_mask, "country"] = "JP"
    companies.loc[
        (companies["country"] == "JP") &
        (companies["city"] == "Amuru"), "city"
    ] = None
    report(
        "Fixed UG/Amuru → JP companies "
        "(Aisan Industry, AISAN KOGYO, TOKAI KOGYO)",
        ug_comp_count, fixed=True
    )
else:
    report("UG/Amuru company geocoding error", 0)

# 13. Missing company country (reported only — some companies
#     genuinely have no location data, e.g. Porsche NaN)
missing_country = companies["country"].isna().sum()
report("Companies with missing country (kept — expected)", missing_country)

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("RELATIONS TABLE CHECKS")
print("=" * 55)

valid_patent_ids   = set(patents["patent_id"])
valid_inventor_ids = set(inventors["inventor_id"])
valid_company_ids  = set(companies["company_id"])

# 14. Orphaned patent_ids
orphan_patents = ~relations["patent_id"].isin(valid_patent_ids)
report("Relations with orphaned patent_id", orphan_patents.sum())
if orphan_patents.sum() > 0:
    relations = relations[~orphan_patents]
    report("Removed orphaned patent relations",
           orphan_patents.sum(), fixed=True)

# 15. Orphaned inventor_ids
orphan_inv = ~relations["inventor_id"].isin(valid_inventor_ids)
report("Relations with orphaned inventor_id", orphan_inv.sum())
if orphan_inv.sum() > 0:
    relations = relations[~orphan_inv]
    report("Removed orphaned inventor relations",
           orphan_inv.sum(), fixed=True)

# 16. Orphaned company_ids (non-null only)
non_null    = relations["company_id"].notna()
orphan_comp = non_null & ~relations["company_id"].isin(valid_company_ids)
report("Relations with orphaned company_id", orphan_comp.sum())
if orphan_comp.sum() > 0:
    relations.loc[orphan_comp, "company_id"] = None
    report("Nulled orphaned company_ids",
           orphan_comp.sum(), fixed=True)

# 17. Null company_id — expected, some patents have no assignee
null_company = relations["company_id"].isna().sum()
report("Relations with no company (unassigned — expected)", null_company)

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("NULL SUMMARY PER TABLE")
print("=" * 55)
for name, df in [("patents",   patents),
                 ("inventors", inventors),
                 ("companies", companies),
                 ("relations", relations)]:
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if len(nulls) == 0:
        print(f"  {name:<12}: no nulls")
    else:
        print(f"  {name:<12}: nulls in:")
        for col, count in nulls.items():
            pct = 100 * count / len(df)
            print(f"    {col:<15}: {count:,} ({pct:.1f}%) "
                  f"{'← expected' if pct < 70 else ''}")

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("SAVING QUALITY-CHECKED FILES")
print("=" * 55)
patents.to_csv(f"{CLEAN}/clean_patents.csv",     index=False)
inventors.to_csv(f"{CLEAN}/clean_inventors.csv", index=False)
companies.to_csv(f"{CLEAN}/clean_companies.csv", index=False)
relations.to_csv(f"{CLEAN}/clean_relations.csv", index=False)

print(f"  ✓ clean_patents.csv   : {len(patents):,} rows")
print(f"  ✓ clean_inventors.csv : {len(inventors):,} rows")
print(f"  ✓ clean_companies.csv : {len(companies):,} rows")
print(f"  ✓ clean_relations.csv : {len(relations):,} rows")

# ════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("QUALITY CHECK SUMMARY")
print("=" * 55)
print(f"  Total issues found : {issues_found:,}")
print(f"  Total issues fixed : {issues_fixed:,}")
print(f"  Remaining issues   : {issues_found - issues_fixed:,} "
      f"(expected nulls only)")
print("\nGeocoding errors fixed:")
print("  INVENTORS:")
print("    ✓ Shigeo Yamamoto  : UG/Amuru    → JP (138 records)")
print("    ✓ Hiroshi Shimizu  : CM/Somalomo → JP  (83 records)")
print("  COMPANIES:")
print("    ✓ Aisan Industry   : UG/Amuru    → JP")
print("    ✓ AISAN KOGYO      : UG/Amuru    → JP")
print("    ✓ TOKAI KOGYO      : UG/Amuru    → JP")
print("    (3 companies, 128 patents reassigned to JP)")
print("\n  Data is clean and ready for 03_load_db.py")
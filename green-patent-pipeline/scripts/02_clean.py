# scripts/02_clean.py
"""
Data processing pipeline that filters raw patent data down to active
GREEN TRANSPORT patents and prepares clean CSVs for database loading.

Focus: CPC Y02T — Climate change mitigation in transportation
  Y02T10 — Road vehicles (EVs, hybrids, fuel cells): ~106,474 patents
  Y02T30 — Aviation fuel efficiency:                    ~1,154 patents
  Y02T50 — Aviation propulsion & materials:            ~29,042 patents
  Y02T70 — Maritime green shipping:                     ~1,775 patents
  Y02T90 — Charging & hydrogen infrastructure:         ~20,770 patents

Filters applied (in order) and why:
  1. CPC Y02T — green transportation patents only
  2. utility  — excludes design/plant patents
  3. withdrawn == 0 — active patents only

Output files saved to data/clean/:
  clean_patents.csv
  clean_inventors.csv
  clean_companies.csv
  clean_relations.csv
"""
import pandas as pd
import os

# ── Force working directory to project root 
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"Working directory: {os.getcwd()}")

RAW   = "data/raw"
CLEAN = "data/clean"
os.makedirs(CLEAN, exist_ok=True)
print(f"Clean folder: {os.path.abspath(CLEAN)}")

# Force patent_id to string — file contains mixed types
# (numeric IDs like 3930271 AND string IDs like "D345393")
# Without this, isin() silently fails
ID_DTYPE = {"patent_id": str}

#Step 1: Y02T transport patent IDs from CPC 
print("\n" + "=" * 55)
print("STEP 1: Filtering green transport patents via CPC Y02T...")
print("=" * 55)
chunks = pd.read_csv(f"{RAW}/g_cpc_current.tsv", sep="\t",
                     usecols=["patent_id", "cpc_group"],
                     dtype=ID_DTYPE, chunksize=500_000)

green_ids = set()
subcategory_counts = {
    "Y02T10": set(), "Y02T30": set(), "Y02T50": set(),
    "Y02T70": set(), "Y02T90": set()
}

for chunk in chunks:
    mask = chunk["cpc_group"].str.startswith("Y02T", na=False)
    green_ids.update(chunk[mask]["patent_id"])
    for sub in subcategory_counts:
        sub_mask = chunk["cpc_group"].str.startswith(sub, na=False)
        subcategory_counts[sub].update(chunk[sub_mask]["patent_id"])

print(f"{len(green_ids):,} Y02T patent IDs found")
print(f"\n  Subcategory breakdown:")
print(f"    Y02T10 Road vehicles (EVs, hybrids, fuel cells): "
      f"{len(subcategory_counts['Y02T10']):,}")
print(f"    Y02T30 Aviation fuel efficiency:                 "
      f"{len(subcategory_counts['Y02T30']):,}")
print(f"    Y02T50 Aviation propulsion & materials:          "
      f"{len(subcategory_counts['Y02T50']):,}")
print(f"    Y02T70 Maritime green shipping:                  "
      f"{len(subcategory_counts['Y02T70']):,}")
print(f"    Y02T90 Charging & hydrogen infrastructure:       "
      f"{len(subcategory_counts['Y02T90']):,}")
print(f"    Note: patents may appear in multiple subcategories")

#Step 2: Core patent info 
print("\n" + "=" * 55)
print("STEP 2: Loading and filtering patents...")
print("=" * 55)
patents = pd.read_csv(f"{RAW}/g_patent.tsv", sep="\t",
                      usecols=["patent_id", "patent_title",
                                "patent_date", "patent_type", "withdrawn"],
                      dtype=ID_DTYPE, low_memory=False)

print(f"  g_patent loaded: {len(patents):,} rows")
print(f"  patent_type values: "
      f"{patents['patent_type'].value_counts().to_dict()}")

# Filter 1: Y02T transport only
patents = patents[patents["patent_id"].isin(green_ids)].copy()
print(f"  After Y02T filter: {len(patents):,} rows")

# Filter 2: Utility only
patents = patents[patents["patent_type"] == "utility"].copy()
print(f"  After utility filter: {len(patents):,} rows")

# Filter 3: Non-withdrawn only
patents["withdrawn"] = patents["withdrawn"].astype(str).str.strip()
print(f"  Withdrawn values: {patents['withdrawn'].value_counts().to_dict()}")
before = len(patents)
patents = patents[patents["withdrawn"] == "0"].copy()
print(f"  After withdrawn filter: {len(patents):,} rows "
      f"(removed {before - len(patents):,})")

# Clean and format
patents["patent_date"] = pd.to_datetime(
    patents["patent_date"], errors="coerce")
patents["year"] = patents["patent_date"].dt.year
patents.dropna(subset=["patent_title"], inplace=True)
patents["patent_title"] = patents["patent_title"].str.strip()
patents.rename(columns={"patent_title": "title",
                         "patent_date":  "filing_date"}, inplace=True)
patents.drop(columns=["withdrawn", "patent_type"], inplace=True)

# Reassign green_ids after ALL filters — keeps downstream tables consistent
green_ids = set(patents["patent_id"])
print(f"  Final active Y02T patent IDs: {len(green_ids):,}")

#Step 3: Abstracts 
print("\n" + "=" * 55)
print("STEP 3: Loading abstracts...")
print("=" * 55)
abstracts = pd.read_csv(f"{RAW}/g_patent_abstract.tsv", sep="\t",
                        dtype=ID_DTYPE, low_memory=False)
abstracts = abstracts[abstracts["patent_id"].isin(green_ids)].copy()
abstracts["patent_abstract"] = (abstracts["patent_abstract"]
                                 .fillna("").str.strip())
abstracts.rename(columns={"patent_abstract": "abstract"}, inplace=True)

patents = patents.merge(abstracts, on="patent_id", how="left")
patents["abstract"] = patents["abstract"].fillna("")

#SAVE patents 
save_path = os.path.join(CLEAN, "clean_patents.csv")
patents.to_csv(save_path, index=False)
print(f"  Saved: {save_path}")
print(f"  Rows: {len(patents):,}")
print(f"  File exists: {os.path.exists(save_path)}")
print(f"  File size: {os.path.getsize(save_path) / (1024**2):.1f} MB")

#Step 4: Location reference table 
# Loaded once, reused for both inventors and companies
print("\n" + "=" * 55)
print("STEP 4: Loading location reference table...")
print("=" * 55)
loc = pd.read_csv(f"{RAW}/g_location_disambiguated.tsv", sep="\t",
                  usecols=["location_id", "disambig_country",
                            "disambig_city", "disambig_state"],
                  dtype={"location_id": str},
                  low_memory=False)
print(f"  {len(loc):,} locations loaded")

#Step 5: Inventors + locations 
print("\n" + "=" * 55)
print("STEP 5: Loading inventors...")
print("=" * 55)
inv = pd.read_csv(f"{RAW}/g_inventor_disambiguated.tsv", sep="\t",
                  usecols=["patent_id", "inventor_id",
                            "disambig_inventor_name_first",
                            "disambig_inventor_name_last",
                            "location_id"],
                  dtype={**ID_DTYPE,
                         "inventor_id": str,
                         "location_id": str},
                  low_memory=False)

inv = inv[inv["patent_id"].isin(green_ids)].copy()
print(f"  Inventor-patent links: {len(inv):,}")

inv = inv.merge(loc, on="location_id", how="left")
inv["name"] = (
    inv["disambig_inventor_name_first"].fillna("") + " " +
    inv["disambig_inventor_name_last"].fillna("")
).str.strip()
inv.rename(columns={"disambig_country": "country",
                     "disambig_city":    "city",
                     "disambig_state":   "state"}, inplace=True)

inv_unique = (inv[["inventor_id", "name", "country", "city", "state"]]
              .drop_duplicates("inventor_id"))

#SAVE inventors 
save_path = os.path.join(CLEAN, "clean_inventors.csv")
inv_unique.to_csv(save_path, index=False)
print(f"  Saved: {save_path}")
print(f"  Rows: {len(inv_unique):,} unique inventors")
print(f"  File exists: {os.path.exists(save_path)}")
print(f"  File size: {os.path.getsize(save_path) / (1024**2):.1f} MB")

total_inv = len(inv_unique)
missing_country = inv_unique["country"].isna().sum()
print(f"  Location merge check:")
print(f"    Missing country: {missing_country:,} "
      f"({100*missing_country/total_inv:.1f}%)")
print(f"    Top 5 countries: "
      f"{inv_unique['country'].value_counts().head().to_dict()}")

#Step 6: Assignees + locations 
print("\n" + "=" * 55)
print("STEP 6: Loading assignees...")
print("=" * 55)
asgn = pd.read_csv(f"{RAW}/g_assignee_disambiguated.tsv", sep="\t",
                   usecols=["patent_id", "assignee_id",
                             "disambig_assignee_organization",
                             "disambig_assignee_individual_name_first",
                             "disambig_assignee_individual_name_last",
                             "assignee_type", "location_id"],
                   dtype={**ID_DTYPE,
                          "assignee_id": str,
                          "location_id": str},
                   low_memory=False)

asgn = asgn[asgn["patent_id"].isin(green_ids)].copy()
print(f"  Assignee-patent links: {len(asgn):,}")

asgn = asgn.merge(loc, on="location_id", how="left")
asgn.rename(columns={"disambig_country": "country",
                      "disambig_city":    "city",
                      "disambig_state":   "state"}, inplace=True)

def resolve_name(row):
    """Prefer org name. Fall back to individual first+last."""
    org = row["disambig_assignee_organization"]
    if pd.notna(org) and str(org).strip():
        return str(org).strip()
    first = str(row["disambig_assignee_individual_name_first"] or "").strip()
    last  = str(row["disambig_assignee_individual_name_last"]  or "").strip()
    return (first + " " + last).strip() or None

asgn["name"] = asgn.apply(resolve_name, axis=1)

asgn_unique = (asgn[["assignee_id", "name", "assignee_type",
                       "country", "city", "state"]]
               .dropna(subset=["name"])
               .drop_duplicates("assignee_id")
               .rename(columns={"assignee_id": "company_id"}))

#SAVE companies 
save_path = os.path.join(CLEAN, "clean_companies.csv")
asgn_unique.to_csv(save_path, index=False)
print(f"  Saved: {save_path}")
print(f"  Rows: {len(asgn_unique):,} unique companies")
print(f"  File exists: {os.path.exists(save_path)}")
print(f"  File size: {os.path.getsize(save_path) / (1024**2):.1f} MB")

total_asgn = len(asgn_unique)
missing_country = asgn_unique["country"].isna().sum()
print(f"  Location merge check:")
print(f"    Missing country: {missing_country:,} "
      f"({100*missing_country/total_asgn:.1f}%)")
print(f"    Top 5 countries: "
      f"{asgn_unique['country'].value_counts().head().to_dict()}")

# Step 7: Relations 
print("\n" + "=" * 55)
print("STEP 7: Building relations table...")
print("=" * 55)
pat_inv  = inv[["patent_id", "inventor_id"]].drop_duplicates()
pat_asgn = (asgn[["patent_id", "assignee_id"]]
            .drop_duplicates()
            .rename(columns={"assignee_id": "company_id"}))

relations = pat_inv.merge(pat_asgn, on="patent_id", how="left")

# SAVE relations 
save_path = os.path.join(CLEAN, "clean_relations.csv")
relations.to_csv(save_path, index=False)
print(f"  Saved: {save_path}")
print(f"  Rows: {len(relations):,}")
print(f"  File exists: {os.path.exists(save_path)}")
print(f"  File size: {os.path.getsize(save_path) / (1024**2):.1f} MB")

#Final Summary 
print("\n" + "=" * 55)
print("PIPELINE COMPLETE — SUMMARY")
print("=" * 55)
print(f"  clean_patents.csv   : {len(patents):,} rows")
print(f"  clean_inventors.csv : {len(inv_unique):,} rows")
print(f"  clean_companies.csv : {len(asgn_unique):,} rows")
print(f"  clean_relations.csv : {len(relations):,} rows")
print(f"\n  All files saved to: {os.path.abspath(CLEAN)}")
print("\nFilters applied:")
print("  CPC Y02T green transportation classification")
print("    (Y02T10 road, Y02T30/50 aviation, "
      "Y02T70 maritime, Y02T90 infrastructure)")
print("  Utility patents only")
print("  Non-withdrawn (active) patents only")
print("\nLocation data:")
print("  Inventors — country, city, state")
print("  Companies — country, city, state")

#Final file verification 
print("\n" + "=" * 55)
print("FILE VERIFICATION")
print("=" * 55)
for fname in ["clean_patents.csv", "clean_inventors.csv",
              "clean_companies.csv", "clean_relations.csv"]:
    fpath = os.path.join(CLEAN, fname)
    if os.path.exists(fpath):
        size = os.path.getsize(fpath) / (1024**2)
        df   = pd.read_csv(fpath, nrows=2)
        print(f"  {fname}")
        print(f"    Size: {size:.1f} MB")
        print(f"    Columns: {list(df.columns)}")
    else:
        print(f"  ✗ MISSING: {fname}")
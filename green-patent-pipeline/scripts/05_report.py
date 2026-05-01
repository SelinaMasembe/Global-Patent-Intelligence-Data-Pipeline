# scripts/05_report.py
"""
Generates three types of reports from the green transport patent database:

  A. Console Report  — formatted terminal output with key findings
  B. CSV Exports     — top_inventors.csv, top_companies.csv,
                       country_trends.csv (already done in 04_analyze.py,
                       this script adds a combined summary CSV)
  C. JSON Report     — structured machine-readable summary

Usage: python3 scripts/05_report.py
"""
import sqlite3
import json
import os
import pandas as pd
from datetime import datetime

# ── Force working directory to project root ───────────────────────────────────
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB  = "patents.db"
OUT = "output"
os.makedirs(OUT, exist_ok=True)

# ── Helper ────────────────────────────────────────────────────────────────────
def q(sql, conn):
    return pd.read_sql_query(sql, conn)

# ── Pull all data needed for reports ─────────────────────────────────────────
with sqlite3.connect(DB) as conn:

    # Total patents
    total_patents = conn.execute(
        "SELECT COUNT(*) FROM patents"
    ).fetchone()[0]

    # Year range
    year_range = conn.execute(
        "SELECT MIN(year), MAX(year) FROM patents WHERE year IS NOT NULL"
    ).fetchone()

    # Total unique inventors
    total_inventors = conn.execute(
        "SELECT COUNT(*) FROM inventors"
    ).fetchone()[0]

    # Total unique companies
    total_companies = conn.execute(
        "SELECT COUNT(*) FROM companies"
    ).fetchone()[0]

    # Top 10 inventors
    top_inv = q("""
        SELECT
            i.name,
            i.country,
            i.city,
            COUNT(DISTINCT r.patent_id) AS patent_count
        FROM inventors i
        JOIN relations r ON i.inventor_id = r.inventor_id
        WHERE i.name IS NOT NULL AND i.name != ''
        GROUP BY i.inventor_id, i.name, i.country, i.city
        ORDER BY patent_count DESC
        LIMIT 10
    """, conn)

    # Top 10 companies
    top_comp = q("""
        SELECT
            c.name,
            c.country,
            COUNT(DISTINCT r.patent_id) AS patent_count
        FROM companies c
        JOIN relations r ON c.company_id = r.company_id
        WHERE c.name IS NOT NULL AND c.name != ''
        GROUP BY c.company_id, c.name, c.country
        ORDER BY patent_count DESC
        LIMIT 10
    """, conn)

    # Top 10 countries by inventor
    top_countries_inv = q("""
        SELECT
            i.country,
            COUNT(DISTINCT r.patent_id)                        AS patent_count,
            ROUND(100.0 * COUNT(DISTINCT r.patent_id) /
                  SUM(COUNT(DISTINCT r.patent_id)) OVER (), 2) AS pct_share
        FROM inventors i
        JOIN relations r ON i.inventor_id = r.inventor_id
        WHERE i.country IS NOT NULL AND i.country != ''
        GROUP BY i.country
        ORDER BY patent_count DESC
        LIMIT 10
    """, conn)

    # Top 10 countries by company
    top_countries_comp = q("""
        SELECT
            c.country,
            COUNT(DISTINCT r.patent_id)                        AS patent_count,
            ROUND(100.0 * COUNT(DISTINCT r.patent_id) /
                  SUM(COUNT(DISTINCT r.patent_id)) OVER (), 2) AS pct_share
        FROM companies c
        JOIN relations r ON c.company_id = r.company_id
        WHERE c.country IS NOT NULL AND c.country != ''
        GROUP BY c.country
        ORDER BY patent_count DESC
        LIMIT 10
    """, conn)

    # Yearly trends
    yearly = q("""
        SELECT year, COUNT(*) AS patent_count
        FROM patents
        WHERE year IS NOT NULL
          AND year BETWEEN 1976 AND 2025
        GROUP BY year
        ORDER BY year
    """, conn)

    # Peak year
    peak_row = yearly.loc[yearly["patent_count"].idxmax()]

    # Y02T subcategory breakdown from yearly
    # Growth calculation (2010 vs 2020)
    count_2010 = yearly[yearly["year"] == "2010"]["patent_count"].values
    count_2020 = yearly[yearly["year"] == "2020"]["patent_count"].values
    growth_pct  = None
    if len(count_2010) > 0 and len(count_2020) > 0:
        growth_pct = round(
            100 * (count_2020[0] - count_2010[0]) / count_2010[0], 1)

    # Patents with no assignee
    unassigned = conn.execute("""
        SELECT COUNT(DISTINCT patent_id) FROM relations
        WHERE company_id IS NULL
    """).fetchone()[0]

# ════════════════════════════════════════════════════════════════════
# A. CONSOLE REPORT
# ════════════════════════════════════════════════════════════════════
WIDTH = 62
now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

report_lines = []

def pr(line=""):
    """Print to terminal and store for saving."""
    print(line)
    report_lines.append(line)

pr("=" * WIDTH)
pr("   GREEN TRANSPORT PATENT INTELLIGENCE REPORT")
pr("   Focus: CPC Y02T — Climate Change Mitigation in Transport")
pr(f"   Generated: {now}")
pr("=" * WIDTH)

pr("")
pr("── DATASET OVERVIEW ──────────────────────────────────────")
pr(f"  Total active green transport patents : {total_patents:>10,}")
pr(f"  Date range                           : "
   f"{year_range[0]} — {year_range[1]}")
pr(f"  Unique inventors                     : {total_inventors:>10,}")
pr(f"  Unique companies / assignees         : {total_companies:>10,}")
pr(f"  Patents with no assignee             : {unassigned:>10,}")
pr(f"  Peak filing year                     : "
   f"{int(peak_row['year'])} "
   f"({int(peak_row['patent_count']):,} patents)")
if growth_pct is not None:
    pr(f"  Growth 2010 → 2020                   : {growth_pct:>9.1f}%")

pr("")
pr("── Y02T SUBCATEGORY COVERAGE ─────────────────────────────")
pr("  Y02T10  Road vehicles (EVs, hybrids, fuel cells) : 106,474")
pr("  Y02T50  Aviation propulsion & materials           :  29,042")
pr("  Y02T90  Charging & hydrogen infrastructure        :  20,770")
pr("  Y02T70  Maritime green shipping                   :   1,775")
pr("  Y02T30  Aviation fuel efficiency                  :   1,154")
pr("  Note: patents may appear in multiple subcategories")

pr("")
pr("── TOP 10 INVENTORS ──────────────────────────────────────")
pr(f"  {'Rank':<5} {'Name':<35} {'Country':<8} {'Patents':>7}")
pr(f"  {'-'*4} {'-'*34} {'-'*7} {'-'*7}")
for i, row in top_inv.iterrows():
    pr(f"  {i+1:<5} {str(row['name']):<35} "
       f"{str(row['country']):<8} {int(row['patent_count']):>7,}")

pr("")
pr("── TOP 10 COMPANIES ──────────────────────────────────────")
pr(f"  {'Rank':<5} {'Company':<38} {'Ctry':<6} {'Patents':>7}")
pr(f"  {'-'*4} {'-'*37} {'-'*5} {'-'*7}")
for i, row in top_comp.iterrows():
    name = str(row["name"])[:37]
    pr(f"  {i+1:<5} {name:<38} "
       f"{str(row['country']):<6} {int(row['patent_count']):>7,}")

pr("")
pr("── TOP 10 COUNTRIES — BY INVENTOR LOCATION ──────────────")
pr("  (Where the research and invention happens)")
pr(f"  {'Rank':<5} {'Country':<10} {'Patents':>8} {'Share':>8}")
pr(f"  {'-'*4} {'-'*9} {'-'*8} {'-'*8}")
for i, row in top_countries_inv.iterrows():
    pr(f"  {i+1:<5} {str(row['country']):<10} "
       f"{int(row['patent_count']):>8,} "
       f"{float(row['pct_share']):>7.2f}%")

pr("")
pr("── TOP 10 COUNTRIES — BY COMPANY LOCATION ───────────────")
pr("  (Where the intellectual property is owned)")
pr(f"  {'Rank':<5} {'Country':<10} {'Patents':>8} {'Share':>8}")
pr(f"  {'-'*4} {'-'*9} {'-'*8} {'-'*8}")
for i, row in top_countries_comp.iterrows():
    pr(f"  {i+1:<5} {str(row['country']):<10} "
       f"{int(row['patent_count']):>8,} "
       f"{float(row['pct_share']):>7.2f}%")

pr("")
pr("── PATENT TRENDS — SELECTED YEARS ───────────────────────")
pr(f"  {'Year':<8} {'Patents':>10} {'Cumulative':>12}")
pr(f"  {'-'*7} {'-'*10} {'-'*12}")
yearly["year"] = yearly["year"].astype(str)
yearly["patent_count"] = yearly["patent_count"].astype(int)
cumulative = 0
milestone_years = [
    "1976","1980","1985","1990","1995","2000",
    "2005","2010","2012","2015","2017","2019",
    "2020","2021","2022","2023","2024","2025"
]
for _, row in yearly.iterrows():
    cumulative += row["patent_count"]
    if str(row["year"]) in milestone_years:
        pr(f"  {str(row['year']):<8} "
           f"{row['patent_count']:>10,} "
           f"{cumulative:>12,}")

pr("")
pr("── KEY INSIGHTS ──────────────────────────────────────────")
pr("  1. USA and Japan dominate both invention and IP ownership")
pr("     in green transport, accounting for ~65% of all patents.")
pr("")
pr("  2. Toyota leads all companies with 9,958 patents —")
pr("     nearly 50% more than second-placed Ford (6,748).")
pr("")
pr("  3. Patent filings grew dramatically after 2010,")
pr("     correlating with the global EV revolution and")
pr("     tightening emissions regulations worldwide.")
pr("")
pr("  4. Inventor country vs company country analysis reveals")
pr("     technology transfer: researchers in one country,")
pr("     IP often owned by multinationals in another.")
pr("")
pr("  5. Data quality: 2 PatentsView geocoding errors were")
pr("     identified and corrected (UG→JP, CM→JP).")
pr("=" * WIDTH)

# ════════════════════════════════════════════════════════════════════
# B. SAVE CONSOLE REPORT AS TXT
# ════════════════════════════════════════════════════════════════════
report_txt_path = f"{OUT}/console_report.txt"
with open(report_txt_path, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))
print(f"\n  → Console report saved: {report_txt_path}")

# ════════════════════════════════════════════════════════════════════
# C. JSON REPORT
# ════════════════════════════════════════════════════════════════════
report_json = {
    "report_title": "Green Transport Patent Intelligence Report",
    "focus": "CPC Y02T — Climate Change Mitigation in Transportation",
    "generated": now,
    "dataset": {
        "total_patents": total_patents,
        "year_range": {
            "from": int(year_range[0]),
            "to":   int(year_range[1])
        },
        "total_inventors": total_inventors,
        "total_companies": total_companies,
        "unassigned_patents": unassigned,
        "peak_year": {
            "year":          int(peak_row["year"]),
            "patent_count":  int(peak_row["patent_count"])
        },
        "growth_2010_to_2020_pct": growth_pct
    },
    "y02t_subcategories": {
        "Y02T10_road_vehicles":          106474,
        "Y02T50_aviation_propulsion":     29042,
        "Y02T90_charging_infrastructure": 20770,
        "Y02T70_maritime":                 1775,
        "Y02T30_aviation_efficiency":      1154,
        "note": "patents may appear in multiple subcategories"
    },
    "top_inventors": [
        {
            "rank":         i + 1,
            "name":         row["name"],
            "country":      row["country"],
            "city":         row["city"],
            "patent_count": int(row["patent_count"])
        }
        for i, row in top_inv.iterrows()
    ],
    "top_companies": [
        {
            "rank":         i + 1,
            "name":         row["name"],
            "country":      row["country"],
            "patent_count": int(row["patent_count"])
        }
        for i, row in top_comp.iterrows()
    ],
    "top_countries_by_inventor": [
        {
            "rank":         i + 1,
            "country":      row["country"],
            "patent_count": int(row["patent_count"]),
            "pct_share":    float(row["pct_share"])
        }
        for i, row in top_countries_inv.iterrows()
    ],
    "top_countries_by_company": [
        {
            "rank":         i + 1,
            "country":      row["country"],
            "patent_count": int(row["patent_count"]),
            "pct_share":    float(row["pct_share"])
        }
        for i, row in top_countries_comp.iterrows()
    ],
    "data_quality": {
        "geocoding_errors_fixed": 2,
        "errors": [
            {
                "inventor": "Shigeo Yamamoto",
                "wrong_country": "UG",
                "wrong_city": "Amuru",
                "corrected_to": "JP",
                "evidence": "Works exclusively for Mitsubishi/Toyota JP companies"
            },
            {
                "inventor": "Hiroshi Shimizu",
                "wrong_country": "CM",
                "wrong_city": "Somalomo",
                "corrected_to": "JP",
                "evidence": "Works exclusively for Denso/Sumitomo JP companies"
            }
        ],
        "missing_inventor_country_pct": 0.7,
        "missing_company_country_pct":  1.7,
        "duplicate_records": 0,
        "orphaned_relations": 0
    }
}

json_path = f"{OUT}/report.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(report_json, f, indent=2)
print(f"  → JSON report saved:    {json_path}")

# ════════════════════════════════════════════════════════════════════
# D. COMBINED SUMMARY CSV
# ════════════════════════════════════════════════════════════════════
summary_rows = [
    {"metric": "Total green transport patents",     "value": total_patents},
    {"metric": "Year range (from)",                 "value": year_range[0]},
    {"metric": "Year range (to)",                   "value": year_range[1]},
    {"metric": "Unique inventors",                  "value": total_inventors},
    {"metric": "Unique companies",                  "value": total_companies},
    {"metric": "Unassigned patents",                "value": unassigned},
    {"metric": "Peak filing year",                  "value": int(peak_row["year"])},
    {"metric": "Peak year patent count",            "value": int(peak_row["patent_count"])},
    {"metric": "Growth 2010-2020 (%)",              "value": growth_pct},
    {"metric": "Top inventor",                      "value": top_inv.iloc[0]["name"]},
    {"metric": "Top inventor patents",              "value": int(top_inv.iloc[0]["patent_count"])},
    {"metric": "Top company",                       "value": top_comp.iloc[0]["name"]},
    {"metric": "Top company patents",               "value": int(top_comp.iloc[0]["patent_count"])},
    {"metric": "Top country (inventor)",            "value": top_countries_inv.iloc[0]["country"]},
    {"metric": "Top country (company)",             "value": top_countries_comp.iloc[0]["country"]},
    {"metric": "Geocoding errors fixed",            "value": 2},
]
summary_df = pd.DataFrame(summary_rows)
summary_path = f"{OUT}/pipeline_summary.csv"
summary_df.to_csv(summary_path, index=False)
print(f"  → Summary CSV saved:    {summary_path}")

print(f"\n{'=' * 62}")
print("REPORT GENERATION COMPLETE")
print(f"{'=' * 62}")
print(f"  output/console_report.txt")
print(f"  output/report.json")
print(f"  output/pipeline_summary.csv")
print(f"  output/top_inventors.csv       (from 04_analyze.py)")
print(f"  output/top_companies.csv       (from 04_analyze.py)")
print(f"  output/country_trends_by_inventor.csv")
print(f"  output/country_trends_by_company.csv")
print(f"  output/yearly_trends.csv")
print(f"  output/inventor_rankings.csv")
print(f"  output/cte_above_average_companies.csv")
print(f"  output/full_join_sample.csv")
# scripts/05_report.py
"""
Generates three types of reports from the green transport patent database:

  A. Console Report  — formatted terminal output with key findings
  B. CSV Exports     — summary CSV
  C. JSON Report     — structured machine-readable summary

Usage: python3 scripts/05_report.py
"""
import sqlite3
import json
import os
import pandas as pd
from datetime import datetime

# ── Force working directory to project root ───────────────────────
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB  = "patents.db"
OUT = "output"
os.makedirs(OUT, exist_ok=True)

def q(sql: str, conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)

# ── Pull all data needed for reports ─────────────────────────────
with sqlite3.connect(DB) as conn:

    total_patents = int(conn.execute(
        "SELECT COUNT(*) FROM patents"
    ).fetchone()[0])

    year_range_row = conn.execute(
        "SELECT MIN(CAST(year AS INTEGER)), "
        "MAX(CAST(year AS INTEGER)) "
        "FROM patents WHERE year IS NOT NULL"
    ).fetchone()
    year_from = int(year_range_row[0])
    year_to   = int(year_range_row[1])

    total_inventors = int(conn.execute(
        "SELECT COUNT(*) FROM inventors"
    ).fetchone()[0])

    total_companies = int(conn.execute(
        "SELECT COUNT(*) FROM companies"
    ).fetchone()[0])

    unassigned = int(conn.execute("""
        SELECT COUNT(DISTINCT patent_id) FROM relations
        WHERE company_id IS NULL
    """).fetchone()[0])

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

    top_countries_inv = q("""
        SELECT
            i.country,
            COUNT(DISTINCT r.patent_id) AS patent_count,
            ROUND(100.0 * COUNT(DISTINCT r.patent_id) /
                  SUM(COUNT(DISTINCT r.patent_id)) OVER (), 2) AS pct_share
        FROM inventors i
        JOIN relations r ON i.inventor_id = r.inventor_id
        WHERE i.country IS NOT NULL AND i.country != ''
        GROUP BY i.country
        ORDER BY patent_count DESC
        LIMIT 10
    """, conn)

    top_countries_comp = q("""
        SELECT
            c.country,
            COUNT(DISTINCT r.patent_id) AS patent_count,
            ROUND(100.0 * COUNT(DISTINCT r.patent_id) /
                  SUM(COUNT(DISTINCT r.patent_id)) OVER (), 2) AS pct_share
        FROM companies c
        JOIN relations r ON c.company_id = r.company_id
        WHERE c.country IS NOT NULL AND c.country != ''
        GROUP BY c.country
        ORDER BY patent_count DESC
        LIMIT 10
    """, conn)

    yearly = q("""
        SELECT year, COUNT(*) AS patent_count
        FROM patents
        WHERE year IS NOT NULL
          AND year BETWEEN 1976 AND 2025
        GROUP BY year
        ORDER BY year
    """, conn)

# ── Extract scalar values safely to avoid Pylance type warnings ───
# Convert year column to int for comparison
yearly["year"]         = yearly["year"].astype(int)  # type: ignore
yearly["patent_count"] = yearly["patent_count"].astype(int)  # type: ignore

peak_idx   = int(yearly["patent_count"].idxmax())  # type: ignore
peak_year  = int(yearly.loc[peak_idx, "year"])  # type: ignore
peak_count = int(yearly.loc[peak_idx, "patent_count"])  # type: ignore

# Growth 2010 → 2020
count_2010_series = yearly.loc[yearly["year"] == 2010, "patent_count"]  # type: ignore
count_2020_series = yearly.loc[yearly["year"] == 2020, "patent_count"]  # type: ignore
growth_pct: float | None = None
if not count_2010_series.empty and not count_2020_series.empty:
    c2010 = int(count_2010_series.iloc[0])  # type: ignore
    c2020 = int(count_2020_series.iloc[0])  # type: ignore
    growth_pct = round(100 * (c2020 - c2010) / c2010, 1)

# Safely extract top inventor and company scalars
top_inv_name    = str(top_inv.iloc[0]["name"])  # type: ignore
top_inv_patents = int(top_inv.iloc[0]["patent_count"])  # type: ignore
top_comp_name   = str(top_comp.iloc[0]["name"])  # type: ignore
top_comp_patents = int(top_comp.iloc[0]["patent_count"])  # type: ignore
top_country_inv  = str(top_countries_inv.iloc[0]["country"])  # type: ignore
top_country_comp = str(top_countries_comp.iloc[0]["country"])  # type: ignore

# ════════════════════════════════════════════════════════════════
# A. CONSOLE REPORT
# ════════════════════════════════════════════════════════════════
WIDTH = 62
now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
report_lines: list[str] = []

def pr(line: str = "") -> None:
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
pr(f"  Date range                           : {year_from} — {year_to}")
pr(f"  Unique inventors                     : {total_inventors:>10,}")
pr(f"  Unique companies / assignees         : {total_companies:>10,}")
pr(f"  Patents with no assignee             : {unassigned:>10,}")
pr(f"  Peak filing year                     : "
   f"{peak_year} ({peak_count:,} patents)")
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
for rank, (_, row) in enumerate(top_inv.iterrows(), start=1):
    name    = str(row["name"])[:34]
    country = str(row["country"]) if pd.notna(row["country"]) else "N/A"
    patents = int(row["patent_count"])
    pr(f"  {rank:<5} {name:<35} {country:<8} {patents:>7,}")

pr("")
pr("── TOP 10 COMPANIES ──────────────────────────────────────")
pr(f"  {'Rank':<5} {'Company':<38} {'Ctry':<6} {'Patents':>7}")
pr(f"  {'-'*4} {'-'*37} {'-'*5} {'-'*7}")
for rank, (_, row) in enumerate(top_comp.iterrows(), start=1):
    name    = str(row["name"])[:37]
    country = str(row["country"]) if pd.notna(row["country"]) else "N/A"
    patents = int(row["patent_count"])
    pr(f"  {rank:<5} {name:<38} {country:<6} {patents:>7,}")

pr("")
pr("── TOP 10 COUNTRIES — BY INVENTOR LOCATION ──────────────")
pr("  (Where the research and invention happens)")
pr(f"  {'Rank':<5} {'Country':<10} {'Patents':>8} {'Share':>8}")
pr(f"  {'-'*4} {'-'*9} {'-'*8} {'-'*8}")
for rank, (_, row) in enumerate(top_countries_inv.iterrows(), start=1):
    country = str(row["country"])
    patents = int(row["patent_count"])
    share   = float(row["pct_share"])
    pr(f"  {rank:<5} {country:<10} {patents:>8,} {share:>7.2f}%")

pr("")
pr("── TOP 10 COUNTRIES — BY COMPANY LOCATION ───────────────")
pr("  (Where the intellectual property is owned)")
pr(f"  {'Rank':<5} {'Country':<10} {'Patents':>8} {'Share':>8}")
pr(f"  {'-'*4} {'-'*9} {'-'*8} {'-'*8}")
for rank, (_, row) in enumerate(top_countries_comp.iterrows(), start=1):
    country = str(row["country"])
    patents = int(row["patent_count"])
    share   = float(row["pct_share"])
    pr(f"  {rank:<5} {country:<10} {patents:>8,} {share:>7.2f}%")

pr("")
pr("── PATENT TRENDS — SELECTED YEARS ───────────────────────")
pr(f"  {'Year':<8} {'Patents':>10} {'Cumulative':>12}")
pr(f"  {'-'*7} {'-'*10} {'-'*12}")
cumulative = 0
milestone_years = {
    1976, 1980, 1985, 1990, 1995, 2000,
    2005, 2010, 2012, 2015, 2017, 2019,
    2020, 2021, 2022, 2023, 2024, 2025
}
for _, row in yearly.iterrows():
    yr  = int(row["year"])
    cnt = int(row["patent_count"])
    cumulative += cnt
    if yr in milestone_years:
        pr(f"  {yr:<8} {cnt:>10,} {cumulative:>12,}")

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
pr("  5. Data quality: 5 PatentsView geocoding errors were")
pr("     identified and corrected across inventors and companies")
pr("     (UG→JP: Yamamoto + 3 companies, CM→JP: Shimizu).")
pr("=" * WIDTH)

# ════════════════════════════════════════════════════════════════
# B. SAVE CONSOLE REPORT AS TXT
# ════════════════════════════════════════════════════════════════
report_txt_path = f"{OUT}/console_report.txt"
with open(report_txt_path, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))
print(f"\n  → Console report saved: {report_txt_path}")

# ════════════════════════════════════════════════════════════════
# C. JSON REPORT
# ════════════════════════════════════════════════════════════════
report_json: dict = {
    "report_title": "Green Transport Patent Intelligence Report",
    "focus": "CPC Y02T — Climate Change Mitigation in Transportation",
    "generated": now,
    "dataset": {
        "total_patents":         total_patents,
        "year_range":            {"from": year_from, "to": year_to},
        "total_inventors":       total_inventors,
        "total_companies":       total_companies,
        "unassigned_patents":    unassigned,
        "peak_year":             {"year": peak_year,
                                  "patent_count": peak_count},
        "growth_2010_to_2020_pct": growth_pct
    },
    "y02t_subcategories": {
        "Y02T10_road_vehicles":           106474,
        "Y02T50_aviation_propulsion":      29042,
        "Y02T90_charging_infrastructure":  20770,
        "Y02T70_maritime":                  1775,
        "Y02T30_aviation_efficiency":       1154,
        "note": "patents may appear in multiple subcategories"
    },
    "top_inventors": [
        {
            "rank":         rank,
            "name":         str(row["name"]),  # type: ignore
            "country":      str(row["country"])  # type: ignore
                            if pd.notna(row["country"]) else None,
            "city":         str(row["city"])  # type: ignore
                            if pd.notna(row["city"]) else None,
            "patent_count": int(row["patent_count"])  # type: ignore
        }
        for rank, (_, row) in enumerate(top_inv.iterrows(), start=1)
    ],
    "top_companies": [
        {
            "rank":         rank,
            "name":         str(row["name"]),  # type: ignore
            "country":      str(row["country"])  # type: ignore
                            if pd.notna(row["country"]) else None,
            "patent_count": int(row["patent_count"])  # type: ignore
        }
        for rank, (_, row) in enumerate(top_comp.iterrows(), start=1)
    ],
    "top_countries_by_inventor": [
        {
            "rank":         rank,
            "country":      str(row["country"]),  # type: ignore
            "patent_count": int(row["patent_count"]),  # type: ignore
            "pct_share":    float(row["pct_share"])  # type: ignore
        }
        for rank, (_, row) in enumerate(
            top_countries_inv.iterrows(), start=1)
    ],
    "top_countries_by_company": [
        {
            "rank":         rank,
            "country":      str(row["country"]),  # type: ignore
            "patent_count": int(row["patent_count"]),  # type: ignore
            "pct_share":    float(row["pct_share"])  # type: ignore
        }
        for rank, (_, row) in enumerate(
            top_countries_comp.iterrows(), start=1)
    ],
    "data_quality": {
        "geocoding_errors_fixed": 5,
        "inventor_errors": [
            {
                "inventor":      "Shigeo Yamamoto",
                "wrong_country": "UG",
                "wrong_city":    "Amuru",
                "corrected_to":  "JP",
                "evidence":      "Works exclusively for Mitsubishi/Toyota"
            },
            {
                "inventor":      "Hiroshi Shimizu",
                "wrong_country": "CM",
                "wrong_city":    "Somalomo",
                "corrected_to":  "JP",
                "evidence":      "Works exclusively for Denso/Sumitomo"
            }
        ],
        "company_errors": [
            {
                "company":       "Aisan Industry Co., Ltd.",
                "wrong_country": "UG",
                "wrong_city":    "Amuru",
                "corrected_to":  "JP",
                "evidence":      "HQ: Obu, Aichi, Japan (est. 1938)"
            },
            {
                "company":       "AISAN KOGYO KABUSHIKI KAISHA",
                "wrong_country": "UG",
                "wrong_city":    "Amuru",
                "corrected_to":  "JP",
                "evidence":      "Japanese corporate name for Aisan Industry"
            },
            {
                "company":       "TOKAI KOGYO CO., LTD.",
                "wrong_country": "UG",
                "wrong_city":    "Amuru",
                "corrected_to":  "JP",
                "evidence":      "Japanese automotive supplier"
            }
        ],
        "missing_inventor_country_pct": 0.7,
        "missing_company_country_pct":  1.7,
        "duplicate_records":            0,
        "orphaned_relations":           0
    }
}

json_path = f"{OUT}/report.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(report_json, f, indent=2)
print(f"  → JSON report saved:    {json_path}")

# ════════════════════════════════════════════════════════════════
# D. COMBINED SUMMARY CSV
# ════════════════════════════════════════════════════════════════
summary_rows = [
    {"metric": "Total green transport patents",  "value": total_patents},
    {"metric": "Year range (from)",              "value": year_from},
    {"metric": "Year range (to)",                "value": year_to},
    {"metric": "Unique inventors",               "value": total_inventors},
    {"metric": "Unique companies",               "value": total_companies},
    {"metric": "Unassigned patents",             "value": unassigned},
    {"metric": "Peak filing year",               "value": peak_year},
    {"metric": "Peak year patent count",         "value": peak_count},
    {"metric": "Growth 2010-2020 (%)",           "value": growth_pct},
    {"metric": "Top inventor",                   "value": top_inv_name},
    {"metric": "Top inventor patents",           "value": top_inv_patents},
    {"metric": "Top company",                    "value": top_comp_name},
    {"metric": "Top company patents",            "value": top_comp_patents},
    {"metric": "Top country (inventor)",         "value": top_country_inv},
    {"metric": "Top country (company)",          "value": top_country_comp},
    {"metric": "Geocoding errors fixed",         "value": 5},
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
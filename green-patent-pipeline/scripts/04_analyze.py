# scripts/04_analyze.py
"""
Runs all 7 required SQL queries against patents.db and saves
results to output/ as CSV files.

Query summary:
  Q1 — Top inventors by patent count
  Q2 — Top companies by patent count
  Q3a — Countries by inventor location
  Q3b — Countries by company location (IP ownership)
  Q4 — Patent trends over time (1976-2025)
  Q5 — Full JOIN across all 4 tables
  Q6 — CTE: companies with above-average patent counts
  Q7 — Window functions: inventor rankings
"""
import sqlite3
import pandas as pd
import os

#Force working directory to project root 
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"Working directory: {os.getcwd()}")

DB  = "patents.db"
OUT = "output"
os.makedirs(OUT, exist_ok=True)

def run_query(conn, label, sql):
    print(f"\n{'=' * 55}")
    print(label)
    print("=" * 55)
    df = pd.read_sql_query(sql, conn)
    print(df.to_string(index=False))
    return df

with sqlite3.connect(DB) as conn:

    #Q1: Top Inventors 
    q1 = run_query(conn, "Q1: TOP 20 INVENTORS — GREEN TRANSPORT PATENTS", """
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
        LIMIT 20
    """)
    q1.to_csv(f"{OUT}/top_inventors.csv", index=False)
    print(f"\n  Saved: output/top_inventors.csv")

    #Q2: Top Companies 
    q2 = run_query(conn, "Q2: TOP 20 COMPANIES — GREEN TRANSPORT PATENTS", """
        SELECT
            c.name,
            c.country,
            COUNT(DISTINCT r.patent_id) AS patent_count
        FROM companies c
        JOIN relations r ON c.company_id = r.company_id
        WHERE c.name IS NOT NULL AND c.name != ''
        GROUP BY c.company_id, c.name, c.country
        ORDER BY patent_count DESC
        LIMIT 20
    """)
    q2.to_csv(f"{OUT}/top_companies.csv", index=False)
    print(f"\n  Saved: output/top_companies.csv")

    #Q3a: Countries by inventor location 
    q3a = run_query(conn,
        "Q3a: COUNTRIES BY INVENTOR LOCATION (where research happens)", """
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
        LIMIT 30
    """)
    q3a.to_csv(f"{OUT}/country_trends_by_inventor.csv", index=False)
    print(f"\n  Saved: output/country_trends_by_inventor.csv")

    #Q3b: Countries by company location 
    q3b = run_query(conn,
        "Q3b: COUNTRIES BY COMPANY LOCATION (where IP is owned)", """
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
        LIMIT 30
    """)
    q3b.to_csv(f"{OUT}/country_trends_by_company.csv", index=False)
    print(f"\n  Saved: output/country_trends_by_company.csv")

    #Q4: Trends over time 
    q4 = run_query(conn,
        "Q4: GREEN TRANSPORT PATENT TRENDS OVER TIME (1976-2025)", """
        SELECT
            year,
            COUNT(*)                             AS patent_count,
            SUM(COUNT(*)) OVER (ORDER BY year)  AS cumulative_total
        FROM patents
        WHERE year IS NOT NULL
          AND year BETWEEN 1976 AND 2025
        GROUP BY year
        ORDER BY year
    """)
    q4.to_csv(f"{OUT}/yearly_trends.csv", index=False)
    print(f"\n  → Saved: output/yearly_trends.csv")

    #Q5: Full JOIN 
    q5 = run_query(conn,
        "Q5: FULL JOIN — PATENTS + INVENTORS + COMPANIES (sample 20)", """
        SELECT
            p.patent_id,
            p.title,
            p.year,
            p.filing_date,
            i.name        AS inventor_name,
            i.country     AS inventor_country,
            i.city        AS inventor_city,
            c.name        AS company_name,
            c.country     AS company_country
        FROM patents p
        JOIN relations  r ON p.patent_id   = r.patent_id
        JOIN inventors  i ON r.inventor_id = i.inventor_id
        LEFT JOIN companies c ON r.company_id = c.company_id
        WHERE p.year IS NOT NULL
        ORDER BY p.year DESC, p.patent_id
        LIMIT 20
    """)
    q5.to_csv(f"{OUT}/full_join_sample.csv", index=False)
    print(f"\n  Saved: output/full_join_sample.csv")

    # Q6: CTE — above-average companies 
    q6 = run_query(conn,
        "Q6: CTE — COMPANIES WITH ABOVE-AVERAGE PATENT COUNTS", """
        WITH company_counts AS (
            SELECT
                c.company_id,
                c.name,
                c.country,
                COUNT(DISTINCT r.patent_id) AS patent_count
            FROM companies c
            JOIN relations r ON c.company_id = r.company_id
            WHERE c.name IS NOT NULL
            GROUP BY c.company_id, c.name, c.country
        ),
        avg_count AS (
            SELECT AVG(patent_count) AS avg_patents
            FROM company_counts
        ),
        above_average AS (
            SELECT
                cc.name,
                cc.country,
                cc.patent_count,
                ROUND(ac.avg_patents, 2)                    AS avg_patents,
                ROUND(cc.patent_count / ac.avg_patents, 2)  AS multiple_of_avg
            FROM company_counts cc
            CROSS JOIN avg_count ac
            WHERE cc.patent_count > ac.avg_patents
        )
        SELECT * FROM above_average
        ORDER BY patent_count DESC
        LIMIT 30
    """)
    q6.to_csv(f"{OUT}/cte_above_average_companies.csv", index=False)
    print(f"\n  Saved: output/cte_above_average_companies.csv")

    #Q7: Ranking with window functions 
    q7 = run_query(conn,
        "Q7: WINDOW FUNCTIONS — INVENTOR RANKINGS", """
        SELECT
            i.name,
            i.country,
            i.city,
            COUNT(DISTINCT r.patent_id)                          AS patent_count,
            RANK() OVER (
                ORDER BY COUNT(DISTINCT r.patent_id) DESC
            )                                                    AS overall_rank,
            DENSE_RANK() OVER (
                PARTITION BY i.country
                ORDER BY COUNT(DISTINCT r.patent_id) DESC
            )                                                    AS rank_in_country,
            ROUND(
                100.0 * COUNT(DISTINCT r.patent_id) /
                SUM(COUNT(DISTINCT r.patent_id)) OVER (), 4
            )                                                    AS pct_of_total
        FROM inventors i
        JOIN relations r ON i.inventor_id = r.inventor_id
        WHERE i.name IS NOT NULL AND i.name != ''
        GROUP BY i.inventor_id, i.name, i.country, i.city
        ORDER BY patent_count DESC
        LIMIT 50
    """)
    q7.to_csv(f"{OUT}/inventor_rankings.csv", index=False)
    print(f"\n  Saved: output/inventor_rankings.csv")

#Summary 
print(f"\n{'=' * 55}")
print("ALL QUERIES COMPLETE")
print("=" * 55)
print("CSV files saved to output/:")
for f in sorted(os.listdir(OUT)):
    fpath = os.path.join(OUT, f)
    size  = os.path.getsize(fpath) / 1024
    print(f"  {f} ({size:.1f} KB)")
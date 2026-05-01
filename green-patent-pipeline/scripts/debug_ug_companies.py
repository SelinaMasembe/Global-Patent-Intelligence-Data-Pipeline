# scripts/debug_ug_companies.py
import sqlite3
import pandas as pd
import os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

with sqlite3.connect("patents.db") as conn:

    print("=" * 55)
    print("Companies with country = UG")
    print("=" * 55)
    df = pd.read_sql_query("""
        SELECT company_id, name, country, city, state
        FROM companies
        WHERE country = 'UG'
    """, conn)
    print(df.to_string(index=False))

    print("\n" + "=" * 55)
    print("Sample patents owned by UG companies")
    print("=" * 55)
    df2 = pd.read_sql_query("""
        SELECT c.name AS company, c.country, c.city,
               p.title, p.year,
               i.name AS inventor, i.country AS inv_country
        FROM companies c
        JOIN relations r ON c.company_id = r.company_id
        JOIN patents p   ON r.patent_id  = p.patent_id
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE c.country = 'UG'
        LIMIT 10
    """, conn)
    print(df2.to_string(index=False))
# scripts/03_load_db.py
"""
Loads clean CSVs into SQLite database and creates indexes.
Run after 02_clean.py: python3 scripts/03_load_db.py

No changes needed when switching from Y02 to Y02T —
this script reads whatever is in data/clean/ automatically.
"""
import sqlite3
import pandas as pd
import os

#Force working directory to project root 
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"Working directory: {os.getcwd()}")

DB    = "patents.db"
CLEAN = "data/clean"
SQL   = "sql/schema.sql"

#Verify clean files exist before starting 
print("\n" + "=" * 55)
print("VERIFYING CLEAN FILES EXIST")
print("=" * 55)
required = ["clean_patents.csv", "clean_inventors.csv",
            "clean_companies.csv", "clean_relations.csv"]
for f in required:
    fpath = os.path.join(CLEAN, f)
    exists = os.path.exists(fpath)
    size   = os.path.getsize(fpath) / (1024**2) if exists else 0
    print(f"  { 'yes' if exists else 'no' } {f} ({size:.1f} MB)")
    if not exists:
        raise FileNotFoundError(
            f"{fpath} not found. Run 02_clean.py first.")

#Create schema 
print("\n" + "=" * 55)
print("CREATING DATABASE SCHEMA")
print("=" * 55)
with sqlite3.connect(DB) as conn:
    with open(SQL) as f:
        conn.executescript(f.read())
print(f"  Schema created in {DB}")

#Load helper 
def load_table(conn, csv_path, table_name, chunksize=100_000):
    print(f"\nLoading {table_name}...")
    total = 0
    for i, chunk in enumerate(pd.read_csv(csv_path,
                                           dtype=str,
                                           chunksize=chunksize)):
        chunk.to_sql(table_name, conn,
                     if_exists="append" if i > 0 else "replace",
                     index=False)
        total += len(chunk)
        print(f"  chunk {i+1}: {total:,} rows loaded", end="\r")
    print(f" {table_name}: {total:,} rows          ")

#Load all tables 
print("\n" + "=" * 55)
print("LOADING TABLES INTO SQLITE")
print("=" * 55)
with sqlite3.connect(DB) as conn:

    load_table(conn, f"{CLEAN}/clean_patents.csv",   "patents")
    load_table(conn, f"{CLEAN}/clean_inventors.csv", "inventors")
    load_table(conn, f"{CLEAN}/clean_companies.csv", "companies")
    load_table(conn, f"{CLEAN}/clean_relations.csv", "relations")

    #Indexes after bulk insert 
    print("\n" + "=" * 55)
    print("CREATING INDEXES")
    print("=" * 55)
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_patents_year      ON patents(year);
        CREATE INDEX IF NOT EXISTS idx_patents_id        ON patents(patent_id);
        CREATE INDEX IF NOT EXISTS idx_inventors_id      ON inventors(inventor_id);
        CREATE INDEX IF NOT EXISTS idx_inventors_country ON inventors(country);
        CREATE INDEX IF NOT EXISTS idx_companies_id      ON companies(company_id);
        CREATE INDEX IF NOT EXISTS idx_companies_country ON companies(country);
        CREATE INDEX IF NOT EXISTS idx_relations_patent  ON relations(patent_id);
        CREATE INDEX IF NOT EXISTS idx_relations_inv     ON relations(inventor_id);
        CREATE INDEX IF NOT EXISTS idx_relations_comp    ON relations(company_id);
    """)
    print("  9 indexes created")

    #Verification 
    print("\n" + "=" * 55)
    print("VERIFICATION")
    print("=" * 55)
    for table in ["patents", "inventors", "companies", "relations"]:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        print(f"  {table:<12}: {count:,} rows")

    #Spot check JOIN 
    print("\nSpot check JOIN (patent + inventor + company):")
    rows = conn.execute("""
        SELECT p.patent_id, p.title, p.year,
               i.name    AS inventor,
               i.country AS inv_country,
               c.name    AS company,
               c.country AS comp_country
        FROM   patents p
        JOIN   relations  r ON p.patent_id   = r.patent_id
        JOIN   inventors  i ON r.inventor_id = i.inventor_id
        LEFT JOIN companies c ON r.company_id = c.company_id
        WHERE  p.year IS NOT NULL
        LIMIT  3
    """).fetchall()
    for row in rows:
        print(f"\n  Patent  : {row[0]} ({row[2]})")
        print(f"  Title   : {row[1][:65]}...")
        print(f"  Inventor: {row[3]} ({row[4]})")
        print(f"  Company : {row[5]} ({row[6]})")

db_size = os.path.getsize(DB) / (1024**2)
print(f"\nDatabase size: {db_size:.1f} MB")
print(f"Database ready -> {os.path.abspath(DB)}")
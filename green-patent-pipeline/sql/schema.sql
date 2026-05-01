-- sql/schema.sql
-- Green Transportation Patent Intelligence Database
-- Focus: CPC Y02T — climate change mitigation in transportation

CREATE TABLE IF NOT EXISTS patents (
    patent_id   TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    abstract    TEXT,
    filing_date TEXT,
    year        INTEGER
);

CREATE TABLE IF NOT EXISTS inventors (
    inventor_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT,
    city        TEXT,
    state       TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    company_id    TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    assignee_type TEXT,
    country       TEXT,
    city          TEXT,
    state         TEXT
);

CREATE TABLE IF NOT EXISTS relations (
    patent_id   TEXT,
    inventor_id TEXT,
    company_id  TEXT,
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id)  REFERENCES companies(company_id)
);
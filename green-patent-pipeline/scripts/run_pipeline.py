# scripts/run_pipeline.py
"""
Full pipeline runner for Green Transport Patent Intelligence.
Runs all steps in order, prints to terminal AND saves complete
log to file.

Usage:
  python3 scripts/run_pipeline.py

PREREQUISITE — Raw data files must be downloaded manually before
running this pipeline. See README.md for full instructions.
Files go in: data/raw/

Outputs:
  Terminal                               — live output as each step runs
  output/logs/pipeline_run_TIMESTAMP.txt — timestamped log
  output/logs/pipeline_latest.txt        — always latest run
"""
import subprocess
import sys
import os
from datetime import datetime

# ── Force working directory to project root ───────────────────────
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Setup ─────────────────────────────────────────────────────────
os.makedirs("output/logs", exist_ok=True)
timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path    = f"output/logs/pipeline_run_{timestamp}.txt"
latest_path = "output/logs/pipeline_latest.txt"

def file_size(path):
    """Return human readable file size or 'not generated'."""
    if not os.path.exists(path):
        return "not generated"
    b = os.path.getsize(path)
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} GB"

STEPS = [
    ("STEP 0 — Verify raw data file columns",
     "scripts/00_check_raw.py"),
    ("STEP 1 — Clean and filter data",
     "scripts/02_clean.py"),
    ("STEP 2 — Data quality checks and fixes",
     "scripts/02b_data_quality.py"),
    ("STEP 3 — Load into SQLite database",
     "scripts/03_load_db.py"),
    ("STEP 4 — Run 7 SQL analysis queries",
     "scripts/04_analyze.py"),
    ("STEP 5 — Generate reports",
     "scripts/05_report.py"),
]

header = f"""
============================================================
  GREEN TRANSPORT PATENT INTELLIGENCE PIPELINE
  Focus  : CPC Y02T — Climate change mitigation in transport
  Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
  Log    : {log_path}
============================================================
"""
lines = [header]
print(header)

failed      = False
failed_step = None

for label, script in STEPS:
    divider = f"""
------------------------------------------------------------
  {label}
  Time: {datetime.now().strftime("%H:%M:%S")}
------------------------------------------------------------"""
    print(divider)
    lines.append(divider)

    process = subprocess.Popen(
        [sys.executable, "-u", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    step_output = []
    if process.stdout:
        for line in process.stdout:
            print(line, end="")
            step_output.append(line)

    process.wait()
    lines.append("".join(step_output))

    if process.returncode != 0:
        msg = (
            f"\n  ✗ FAILED: {label} "
            f"(exit code {process.returncode})\n"
            f"  Pipeline stopped.\n"
        )
        print(msg)
        lines.append(msg)
        failed      = True
        failed_step = label
        break
    else:
        msg = f"\n  ✓ COMPLETED: {label}\n"
        print(msg)
        lines.append(msg)

# ── Footer ────────────────────────────────────────────────────────
status = (f"FAILED at: {failed_step}"
          if failed else
          "ALL STEPS COMPLETED SUCCESSFULLY")

footer = f"""
============================================================
  PIPELINE {status}
  Finished: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
============================================================

Generated files:
  data/clean/clean_patents.csv     {file_size("data/clean/clean_patents.csv")}
  data/clean/clean_inventors.csv   {file_size("data/clean/clean_inventors.csv")}
  data/clean/clean_companies.csv   {file_size("data/clean/clean_companies.csv")}
  data/clean/clean_relations.csv   {file_size("data/clean/clean_relations.csv")}
  patents.db                       {file_size("patents.db")}
  output/top_inventors.csv         {file_size("output/top_inventors.csv")}
  output/top_companies.csv         {file_size("output/top_companies.csv")}
  output/yearly_trends.csv         {file_size("output/yearly_trends.csv")}
  output/report.json               {file_size("output/report.json")}
  output/console_report.txt        {file_size("output/console_report.txt")}
  {log_path}
============================================================
"""
print(footer)
lines.append(footer)

full_log = "".join(lines)
with open(log_path, "w", encoding="utf-8") as f:
    f.write(full_log)
with open(latest_path, "w", encoding="utf-8") as f:
    f.write(full_log)

print(f"Log saved to : {log_path}")
print(f"Latest log   : {latest_path}")
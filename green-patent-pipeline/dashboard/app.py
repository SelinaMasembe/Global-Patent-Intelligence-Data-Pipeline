# dashboard/app.py
"""
Green Transport Patent Intelligence Dashboard
Streamlit interactive dashboard for exploring Y02T patent data.

Usage: streamlit run dashboard/app.py
"""
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ── Force working directory to project root ───────────────────────────────────
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB = "patents.db"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Green Transport Patent Intelligence",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS for modern styling ──────────────────────────────────────────────
st.markdown("""
<style>
:root {
    --primary-green: #10b981;
    --dark-text: #1f2937;
    --light-gray: #f9fafb;
    --border-gray: #e5e7eb;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
}

.stTitle {
    color: var(--dark-text);
    font-weight: 600;
}

.stSubheader {
    color: var(--dark-text);
    font-weight: 500;
    margin-top: 24px;
    margin-bottom: 12px;
}

.metric-card {
    background: white;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid var(--border-gray);
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}

hr {
    border: none;
    height: 1px;
    background: var(--border-gray);
    margin: 24px 0;
}
</style>
""", unsafe_allow_html=True)

# ── DB helper ─────────────────────────────────────────────────────────────────
@st.cache_data
def query(sql):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql_query(sql, conn)

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("Green Transport Patents")
st.sidebar.markdown("Explore climate change mitigation technology in transportation (CPC Y02T) from the USPTO PatentsView database.")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["Overview",
     "Trends Over Time",
     "Top Inventors",
     "Top Companies",
     "Country Analysis",
     "Patent Explorer"]
)

st.sidebar.divider()
st.sidebar.markdown("**Y02T Categories**")
st.sidebar.markdown("""
- Road vehicles (106K patents)
- Aviation propulsion (29K)
- Charging infrastructure (21K)
- Maritime (2K)
- Aviation efficiency (1K)
""")

# ════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ════════════════════════════════════════════════════════════════════
if page == "Overview":
    st.title("Green Transport Patent Intelligence")
    st.markdown("Comprehensive analysis of **141,124 active patents** in climate-friendly transportation technology (CPC Y02T), spanning 1976–2025.")

    # ── KPI cards ─────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)

    total = query("SELECT COUNT(*) as n FROM patents").iloc[0]["n"]
    inventors = query("SELECT COUNT(*) as n FROM inventors").iloc[0]["n"]
    companies = query("SELECT COUNT(*) as n FROM companies").iloc[0]["n"]
    peak = query("""
        SELECT year, COUNT(*) as n FROM patents
        WHERE year IS NOT NULL
        GROUP BY year ORDER BY n DESC LIMIT 1
    """).iloc[0]
    growth = query("""
        SELECT year, COUNT(*) as n FROM patents
        WHERE year IN ('2010','2020')
        GROUP BY year ORDER BY year
    """)

    col1.metric("Total Patents", f"{int(total):,}")
    col2.metric("Inventors", f"{int(inventors):,}")
    col3.metric("Companies", f"{int(companies):,}")
    col4.metric("Peak Year", f"{int(peak['year'])}", f"{int(peak['n']):,} patents")
    if len(growth) == 2:
        g = round(100*(growth.iloc[1]["n"] - growth.iloc[0]["n"])
                  / growth.iloc[0]["n"], 1)
        col5.metric("Growth 2010→2020", f"{g}%")

    st.divider()

    # ── Quick overview charts side by side ────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Patent Growth Over Time")
        trend = query("""
            SELECT year, COUNT(*) as patents
            FROM patents
            WHERE year IS NOT NULL
              AND year BETWEEN 1976 AND 2025
            GROUP BY year ORDER BY year
        """)
        trend["year"] = trend["year"].astype(int)
        fig = px.area(trend, x="year", y="patents",
                      color_discrete_sequence=["#10b981"],
                      labels={"year": "Year",
                               "patents": "Patents Granted"})
        fig.update_layout(showlegend=False, height=300,
                          margin=dict(t=10, b=10),
                          plot_bgcolor="rgba(249,250,251,1)",
                          paper_bgcolor="white")
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(229,231,235,0.5)")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Top 10 Countries (Inventor Location)")
        countries = query("""
            SELECT i.country,
                   COUNT(DISTINCT r.patent_id) AS patents
            FROM inventors i
            JOIN relations r ON i.inventor_id = r.inventor_id
            WHERE i.country IS NOT NULL AND i.country != ''
            GROUP BY i.country
            ORDER BY patents DESC LIMIT 10
        """)
        fig2 = px.bar(countries, x="patents", y="country",
                      orientation="h",
                      color="patents",
                      color_continuous_scale="Greens",
                      labels={"country": "Country",
                               "patents": "Patents"})
        fig2.update_layout(showlegend=False, height=300,
                           yaxis=dict(autorange="reversed"),
                           margin=dict(t=10, b=10),
                           plot_bgcolor="rgba(249,250,251,1)",
                           paper_bgcolor="white",
                           coloraxis=dict(showscale=False))
        fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(229,231,235,0.5)")
        st.plotly_chart(fig2, use_container_width=True)

    # ── Subcategory breakdown ──────────────────────────────────────
    st.subheader("Technology Distribution by Category")
    subcat = pd.DataFrame({
        "Category": [
            "Road Vehicles",
            "Aviation Propulsion",
            "Charging Infrastructure",
            "Maritime",
            "Aviation Efficiency"
        ],
        "Patents": [106474, 29042, 20770, 1775, 1154]
    })
    fig3 = px.pie(subcat, values="Patents", names="Category",
                  color_discrete_sequence=["#10b981", "#059669", "#047857", "#065f46", "#064e3b"],
                  hole=0.4)
    fig3.update_layout(height=350)
    st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════════
# PAGE 2 — TRENDS OVER TIME
# ════════════════════════════════════════════════════════════════════
elif page == "Trends Over Time":
    st.title("Patent Trends Over Time")
    st.markdown("Track the evolution of green transport innovation from 1976 to 2025.")

    trend = query("""
        SELECT year, COUNT(*) as patents,
               SUM(COUNT(*)) OVER (ORDER BY year) as cumulative
        FROM patents
        WHERE year IS NOT NULL
          AND year BETWEEN 1976 AND 2025
        GROUP BY year ORDER BY year
    """)
    trend["year"] = trend["year"].astype(int)

    # Year range slider
    yr_min, yr_max = st.slider(
        "Select year range",
        min_value=1976, max_value=2025,
        value=(1990, 2025),
        key="trends_year_range"
    )
    filtered = trend[
        (trend["year"] >= yr_min) &
        (trend["year"] <= yr_max)
    ]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Annual Patent Filings")
        fig = px.bar(filtered, x="year", y="patents",
                     color="patents",
                     color_continuous_scale="Greens",
                     labels={"year": "Year",
                              "patents": "Patents Granted"})
        fig.update_layout(height=400, plot_bgcolor="rgba(249,250,251,1)", paper_bgcolor="white",
                          coloraxis=dict(showscale=False))
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(229,231,235,0.5)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Cumulative Patents")
        fig2 = px.area(filtered, x="year", y="cumulative",
                       color_discrete_sequence=["#10b981"],
                       labels={"year": "Year",
                                "cumulative": "Cumulative Patents"})
        fig2.update_layout(showlegend=False, height=400, plot_bgcolor="rgba(249,250,251,1)", paper_bgcolor="white")
        fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(229,231,235,0.5)")
        st.plotly_chart(fig2, use_container_width=True)

    # Key milestones annotation
    st.subheader("Key Milestones")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Patents by 2000",  f"{filtered[filtered['year']<=2000]['patents'].sum():,}")
    col2.metric("Patents by 2010",  f"{filtered[filtered['year']<=2010]['patents'].sum():,}")
    col3.metric("Patents by 2020",  f"{filtered[filtered['year']<=2020]['patents'].sum():,}")
    peak_idx = int(trend['patents'].idxmax())
    peak_year = int(trend.iloc[peak_idx]['year'])
    col4.metric("Peak Year", f"{peak_year}", f"{int(trend['patents'].max()):,} patents")

    st.dataframe(filtered.rename(columns={
        "year": "Year",
        "patents": "Patents",
        "cumulative": "Cumulative"
    }), use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════════
# PAGE 3 — TOP INVENTORS
# ════════════════════════════════════════════════════════════════════
elif page == "Top Inventors":
    st.title("Top Green Transport Inventors")

    n = st.slider("Number of inventors to show", 10, 50, 20, key="top_inventors_count")
    country_filter = st.multiselect(
        "Filter by country",
        options=query("""
            SELECT DISTINCT i.country FROM inventors i
            JOIN relations r ON i.inventor_id = r.inventor_id
            WHERE i.country IS NOT NULL
            ORDER BY i.country
        """)["country"].tolist(),
        default=[]
    )

    country_clause = ""
    if country_filter:
        countries_str = "','".join(country_filter)
        country_clause = f"AND i.country IN ('{countries_str}')"

    inv = query(f"""
        SELECT i.name, i.country, i.city,
               COUNT(DISTINCT r.patent_id) AS patents
        FROM inventors i
        JOIN relations r ON i.inventor_id = r.inventor_id
        WHERE i.name IS NOT NULL AND i.name != ''
        {country_clause}
        GROUP BY i.inventor_id, i.name, i.country, i.city
        ORDER BY patents DESC
        LIMIT {n}
    """)

    col1, col2 = st.columns([3, 2])

    with col1:
        fig = px.bar(inv, x="patents", y="name",
                     orientation="h",
                     color="country",
                     labels={"name": "Inventor",
                              "patents": "Patents",
                              "country": "Country"},
                     height=max(400, n * 22))
        fig.update_layout(yaxis=dict(autorange="reversed"), plot_bgcolor="rgba(249,250,251,1)", paper_bgcolor="white")
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(229,231,235,0.5)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Country Distribution")
        country_dist = inv["country"].value_counts().reset_index()
        country_dist.columns = ["Country", "Inventors"]
        fig2 = px.pie(country_dist, values="Inventors",
                      names="Country",
                      color_discrete_sequence=["#10b981", "#059669", "#047857", "#065f46", "#064e3b"])
        fig2.update_layout(height=350)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Inventor Rankings")
    inv_display = inv.copy()
    inv_display.insert(0, "Rank", range(1, len(inv_display)+1))
    st.dataframe(inv_display.rename(columns={
        "name": "Inventor", "country": "Country",
        "city": "City", "patents": "Patents", "Rank": "Rank"
    }), use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════════
# PAGE 4 — TOP COMPANIES
# ════════════════════════════════════════════════════════════════════
elif page == "Top Companies":
    st.title("Top Green Transport Companies")

    n = st.slider("Number of companies to show", 10, 50, 20, key="top_companies_count")

    comp = query(f"""
        SELECT c.name, c.country,
               COUNT(DISTINCT r.patent_id) AS patents
        FROM companies c
        JOIN relations r ON c.company_id = r.company_id
        WHERE c.name IS NOT NULL AND c.name != ''
        GROUP BY c.company_id, c.name, c.country
        ORDER BY patents DESC
        LIMIT {n}
    """)

    fig = px.bar(comp, x="patents", y="name",
                 orientation="h",
                 color="country",
                 labels={"name": "Company",
                          "patents": "Patents",
                          "country": "Country"},
                 height=max(400, n * 22))
    fig.update_layout(yaxis=dict(autorange="reversed"), plot_bgcolor="rgba(249,250,251,1)", paper_bgcolor="white")
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(229,231,235,0.5)")
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Companies by Country")
        country_comp = comp.groupby("country")["patents"].sum().reset_index()
        country_comp = country_comp.sort_values("patents", ascending=False)
        fig2 = px.pie(country_comp, values="patents", names="country",
                      color_discrete_sequence=["#10b981", "#059669", "#047857", "#065f46", "#064e3b"],
                      hole=0.3)
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.subheader("Above-Average Companies (CTE Result)")
        above_avg = query("""
            WITH counts AS (
                SELECT c.name, c.country,
                       COUNT(DISTINCT r.patent_id) AS patents
                FROM companies c
                JOIN relations r ON c.company_id = r.company_id
                GROUP BY c.company_id, c.name, c.country
            ),
            avg_c AS (SELECT AVG(patents) AS avg FROM counts)
            SELECT name, country, patents,
                   ROUND(patents / avg, 1) AS multiple_of_avg
            FROM counts, avg_c
            WHERE patents > avg
            ORDER BY patents DESC LIMIT 15
        """)
        st.dataframe(above_avg.rename(columns={
            "name": "Company", "country": "Country",
            "patents": "Patents", "multiple_of_avg": "× Avg"
        }), use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════════
# PAGE 5 — COUNTRY ANALYSIS
# ════════════════════════════════════════════════════════════════════
elif page == "Country Analysis":
    st.title("Country Analysis")
    st.markdown("Compare research origins (inventor location) versus IP ownership (company location) to understand global technology transfer patterns.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("By Inventor Location")
        st.caption("Where research and invention originates")
        inv_countries = query("""
            SELECT i.country,
                   COUNT(DISTINCT r.patent_id) AS patents,
                   ROUND(100.0 * COUNT(DISTINCT r.patent_id) /
                         SUM(COUNT(DISTINCT r.patent_id)) OVER (), 2) AS pct
            FROM inventors i
            JOIN relations r ON i.inventor_id = r.inventor_id
            WHERE i.country IS NOT NULL AND i.country != ''
            GROUP BY i.country
            ORDER BY patents DESC LIMIT 15
        """)
        fig = px.bar(inv_countries, x="country", y="patents",
                     color="patents",
                     color_continuous_scale="Greens",
                     text="pct",
                     labels={"country": "Country",
                              "patents": "Patents",
                              "pct": "Share %"})
        fig.update_traces(texttemplate="%{text}%",
                          textposition="outside")
        fig.update_layout(coloraxis_showscale=False, height=450, plot_bgcolor="rgba(249,250,251,1)", paper_bgcolor="white")
        fig.update_xaxes(showgrid=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("By Company Location")
        st.caption("Where intellectual property is owned")
        comp_countries = query("""
            SELECT c.country,
                   COUNT(DISTINCT r.patent_id) AS patents,
                   ROUND(100.0 * COUNT(DISTINCT r.patent_id) /
                         SUM(COUNT(DISTINCT r.patent_id)) OVER (), 2) AS pct
            FROM companies c
            JOIN relations r ON c.company_id = r.company_id
            WHERE c.country IS NOT NULL AND c.country != ''
            GROUP BY c.country
            ORDER BY patents DESC LIMIT 15
        """)
        fig2 = px.bar(comp_countries, x="country", y="patents",
                      color="patents",
                      color_continuous_scale="Greens",
                      text="pct",
                      labels={"country": "Country",
                               "patents": "Patents",
                               "pct": "Share %"})
        fig2.update_traces(texttemplate="%{text}%",
                           textposition="outside")
        fig2.update_layout(coloraxis_showscale=False, height=450, plot_bgcolor="rgba(249,250,251,1)", paper_bgcolor="white")
        fig2.update_xaxes(showgrid=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Comparison table
    st.subheader("Inventor vs Company Country Comparison")
    merged = inv_countries.merge(
        comp_countries,
        on="country", suffixes=("_inventor", "_company")
    )
    merged["difference"] = (merged["patents_inventor"] -
                             merged["patents_company"])
    st.dataframe(merged[[
        "country", "patents_inventor", "pct_inventor",
        "patents_company", "pct_company", "difference"
    ]].rename(columns={
        "country":          "Country",
        "patents_inventor": "Inv. Patents",
        "pct_inventor":     "Inv. %",
        "patents_company":  "Co. Patents",
        "pct_company":      "Co. %",
        "difference":       "Difference"
    }), use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════════
# PAGE 6 — PATENT EXPLORER
# ════════════════════════════════════════════════════════════════════
elif page == "Patent Explorer":
    st.title("Patent Explorer")
    st.markdown("Search and explore green transport patents in detail.")

    col1, col2, col3 = st.columns(3)
    with col1:
        search_term = st.text_input("Search patent title", "")
    with col2:
        year_from = st.number_input("Year from", 1976, 2025, 2015)
    with col3:
        year_to = st.number_input("Year to", 1976, 2025, 2025)

    where_clauses = [
        f"p.year >= '{year_from}'",
        f"p.year <= '{year_to}'"
    ]
    if search_term:
        where_clauses.append(
            f"LOWER(p.title) LIKE LOWER('%{search_term}%')"
        )

    where_sql = " AND ".join(where_clauses)

    results = query(f"""
        SELECT DISTINCT
            p.patent_id,
            p.title,
            p.year,
            p.filing_date,
            c.name    AS company,
            c.country AS company_country
        FROM patents p
        LEFT JOIN relations r  ON p.patent_id   = r.patent_id
        LEFT JOIN companies c  ON r.company_id  = c.company_id
        WHERE {where_sql}
        ORDER BY p.year DESC, p.patent_id
        LIMIT 200
    """)

    st.markdown(f"**{len(results):,} patents found** "
                f"(showing up to 200)")

    st.dataframe(results.rename(columns={
        "patent_id":        "Patent ID",
        "title":            "Title",
        "year":             "Year",
        "filing_date":      "Filing Date",
        "company":          "Company",
        "company_country":  "Country"
    }), use_container_width=True, hide_index=True)
import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd
import streamlit as st
import plotly.express as px
# from snowflake.snowpark.context import get_active_session # get_active_session() only works inside Snowflake. Locally, you need to replace it with a real Snowflake connection.


# ---------------------------------------------------------------------------
# SETUP - LOCAL
# A Session is the entry point to Snowpark. Think of it like a database
# connection, but it also lets you write Python that runs inside Snowflake.
# ---------------------------------------------------------------------------

load_dotenv()

def get_session():
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()

session = get_session()


# ---------------------------------------------------------------------------
# SETUP - SNOWFLAKE STREAMLIT
# get_active_session() works inside Snowflake's Streamlit environment.
# It connects automatically using the app's own Snowflake context,
# so no credentials are needed here.
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SA Rental Stress", layout="wide")

st.title("SA Rental Stress Dashboard")
st.caption("Data: SA Government Private Rental Report (latest until 2025 Q4) · Income baseline: ABS 2021 median $1,889/wk")

# session = get_active_session()


# ---------------------------------------------------------------------------
# SHARED LAYOUT CONFIG
# Applied to every Plotly chart so height and margins are consistent.
# Change height here to update all charts at once.
# ---------------------------------------------------------------------------

base_layout = dict(
    height=600,
    margin=dict(l=0, r=0, t=10, b=0),
    font=dict(size=14),
)


# ---------------------------------------------------------------------------
# LOAD DATA
# We load both mart tables once and cache them so the dashboard is fast.
# ---------------------------------------------------------------------------

@st.cache_data
def load_region():
    return session.table("RENTAL_STRESS.MART.MART_REGION_STRESS").to_pandas()

@st.cache_data
def load_suburb_expensive():
    return session.table("RENTAL_STRESS.MART.MART_SUBURB_EXPENSIVE").to_pandas()

@st.cache_data
def load_suburb_affordable():
    return session.table("RENTAL_STRESS.MART.MART_SUBURB_AFFORDABLE").to_pandas()

region_df = load_region()
expensive_df = load_suburb_expensive()
affordable_df = load_suburb_affordable()

# Normalise column names to lowercase so the rest of the code is consistent
region_df.columns = region_df.columns.str.lower()
expensive_df.columns = expensive_df.columns.str.lower()
affordable_df.columns = affordable_df.columns.str.lower()


# ---------------------------------------------------------------------------
# SECTION 1: TREND
# Line chart showing median weekly rent by region across all 4 quarters.
# We melt the pivoted data into long format, which is what Plotly expects:
# one row per (quarter, region) pair.
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Median weekly rent by region 2025 Quarterly trend")

# Pivot so each region is a column, then melt back to long format for Plotly
trend_df = region_df[["quarter", "region", "total_median"]].copy()
trend_pivot = trend_df.pivot(index="quarter", columns="region", values="total_median")
trend_pivot = trend_pivot.sort_index().reset_index()
trend_long = trend_pivot.melt(id_vars="quarter", var_name="region", value_name="total_median")

# Draw the line chart. Y-axis starts at 200 to focus on the relevant range.
fig = px.line(trend_long, x="quarter", y="total_median", color="region", markers=True)
fig.update_layout(
    **base_layout,
    yaxis=dict(range=[200, trend_long["total_median"].max() + 50]),
    xaxis_title=None,
    yaxis_title="Median rent ($/wk)",
    legend_title=None,
)
st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# SECTION 2: SUBURBS
# Two bar charts side by side: most expensive and most affordable suburbs.
# Only includes suburbs with 10+ bonds lodged (filtered in transform.py).
# Bars are horizontal so suburb names are readable on the y-axis.
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Suburb rent 2025 Q4")
st.caption("Suburbs with 10 or more bonds lodged. One bond = one rental dwelling.")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Top 10 most expensive suburbs**")
    # Sort descending so the most expensive suburb appears at the top
    exp = expensive_df[["suburb", "total_median"]].sort_values("total_median", ascending=True)
    fig = px.bar(exp, x="total_median", y="suburb", orientation="h", color_discrete_sequence=["#e67e22"])
    fig.update_layout(
        **base_layout,
        xaxis_title="Median rent ($/wk)",
        yaxis_title=None,
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Top 10 most affordable suburbs**")
    # Sort ascending so the cheapest suburb appears at the top
    aff = affordable_df[["suburb", "total_median"]].sort_values("total_median", ascending=False)
    fig = px.bar(aff, x="total_median", y="suburb", orientation="h", color_discrete_sequence=["#2ecc71"])
    fig.update_layout(
        **base_layout,
        xaxis_title="Median rent ($/wk)",
        yaxis_title=None,
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# SECTION 3: RENTAL STRESS
# Shows which regions exceed the 30% rental stress threshold.
# Uses Q4 2025 data only.
#
# Rental stress = (median weekly rent / median weekly income) * 100
# Threshold = 30% (standard used by Australian government and researchers)
# Income used = $1,889/wk (ABS 2021 SA median family income)
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Rental stress by region 2025 Q4")
st.caption("Rental stress = household spending more than 30% of income on rent.")

# Filter to Q4 only
q4_df = region_df[region_df["quarter"] == "2025-Q4"].copy()

# --- Metric cards ---
stressed_count = int(q4_df["stressed"].sum())
total_count = len(q4_df)
worst_region = q4_df.loc[q4_df["stress_pct"].idxmax(), "region"]
best_region = q4_df.loc[q4_df["stress_pct"].idxmin(), "region"]

m1, m2, m3 = st.columns(3)
m1.markdown(f"Regions in stress (>30%)\n\n<span style='color:#e74c3c; font-size:2rem; font-weight:700;'>{stressed_count} of {total_count}</span>", unsafe_allow_html=True)
m2.markdown(f"Worst region\n\n<span style='color:#e74c3c; font-size:1.5rem; font-weight:700;'>{worst_region}</span>", unsafe_allow_html=True)
m3.markdown(f"Best region\n\n<span style='color:#2ecc71; font-size:1.5rem; font-weight:700;'>{best_region}</span>", unsafe_allow_html=True)

# --- Colour map ---
# Maps each stress label to a consistent colour used across the chart and badges
color_map = {"high stress": "#e74c3c", "moderate": "#e67e22", "affordable": "#2ecc71"}

# Sort ascending so the highest stress region appears at the top of the horizontal bar
q4_sorted = q4_df.sort_values("stress_pct", ascending=True)
q4_sorted["color"] = q4_sorted["stress_label"].map(color_map)

col_chart, col_table = st.columns(2)

with col_chart:
    st.markdown("**% of median income spent on rent**")
    # Colour each bar by stress label using color_discrete_map
    fig = px.bar(
        q4_sorted,
        x="stress_pct",
        y="region",
        orientation="h",
        color="stress_label",
        color_discrete_map=color_map,
    )
    fig.update_layout(
        **base_layout,
        xaxis_title=None,
        yaxis_title=None,
        xaxis_tickformat=".1f",
        xaxis_ticksuffix="%",
        showlegend=False,
    )
    fig.update_traces(hovertemplate="%{y}: %{x:.1%}<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)

with col_table:
    st.markdown("**Stress level by region**")

    # Badge background and text colours per stress label
    badge_colors = {
        "high stress": ("#fde8e8", "#e74c3c"),
        "moderate":    ("#fef3e2", "#e67e22"),
        "affordable":  ("#e8f8f0", "#2ecc71"),
    }

    # Sort descending so highest stress region appears at the top of the list
    table_df = q4_sorted.sort_values("stress_pct", ascending=False)[["region", "stress_pct", "stress_label"]]

    # Render each row as a styled HTML row with a colour-coded badge
    for _, row in table_df.iterrows():
        bg, fg = badge_colors.get(row["stress_label"], ("#eee", "#333"))
        pct = f"{row['stress_pct']:.0f}%"
        label = row["stress_label"]
        region_name = row["region"]

        st.markdown(
            f"""
            <div style="display:flex; justify-content:space-between; align-items:center;
                        padding: 10px 4px; border-bottom: 1px solid #2a2a2a;">
                <span style="font-size:15px;">{region_name}</span>
                <span style="background:{bg}; color:{fg}; padding:4px 12px;
                             border-radius:20px; font-size:13px; font-weight:500;">
                    {pct} ({label})
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

# --- Full breakdown table ---
# Shows all regions with median rent, stress %, and stress label
st.markdown("**Full breakdown**")
display_df = q4_df[["region", "total_median", "stress_pct", "stress_label"]].copy()
display_df.columns = ["Region", "Median rent ($/wk)", "% of income on rent", "Stress level"]
display_df = display_df.sort_values("% of income on rent", ascending=False).reset_index(drop=True)
st.dataframe(display_df, use_container_width=True, hide_index=True, height=450)
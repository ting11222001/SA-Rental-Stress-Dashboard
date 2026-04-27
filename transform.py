import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, round as sp_round, when

load_dotenv()

# ---------------------------------------------------------------------------
# SNOWPARK BASICS
# A Session is the entry point to Snowpark. Think of it like a database
# connection, but it also lets you write Python that runs inside Snowflake.
# ---------------------------------------------------------------------------

def get_session():
    return Session.builder.configs({
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database":  os.getenv("SNOWFLAKE_DATABASE"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
    }).create()

 
# ---------------------------------------------------------------------------
# CONSTANTS
# The ABS 2021 median family income for South Australia, in dollars per week.
# We use this to calculate what percentage of income goes to rent.
# ---------------------------------------------------------------------------

MEDIAN_INCOME_WEEKLY = 1889

 
# ---------------------------------------------------------------------------
# STEP 1: CLEAN THE REGION DATA
# Read raw region rows, drop nulls, add rental stress calculation.
#
# Snowpark DataFrames are lazy — nothing runs in Snowflake until you call
# .write_pandas() or .save_as_table(). Each line below just builds up a
# query plan.
# ---------------------------------------------------------------------------

def transform_region(session):
    df = session.table("RENTAL_STRESS.RAW.RAW_REGION")

    # Drop any rows where median rent is missing
    df = df.filter(col("TOTAL_MEDIAN").is_not_null())   # col("NAME") means use this column in my calculation
    df = df.filter(col("TOTAL_COUNT").is_not_null())

  
    # Calculate stress percentage: (rent / income) * 100
    df = df.with_column(
        "STRESS_PCT",
        sp_round((col("TOTAL_MEDIAN") / lit(MEDIAN_INCOME_WEEKLY)) * lit(100), 1)  # round as sp_round rounds a number to X decimal places. Renamed to sp_round to avoid clashing with Python's built-in round()
    )

    # Flag regions where stress exceeds the 30% threshold
    df = df.with_column(
        "STRESSED",
        when(col("STRESS_PCT") > lit(30), lit(True)).otherwise(lit(False))  # lit(value) wraps a plain Python value (a number, a string) so Snowpark understands it.
    )

    # Add a label column for the dashboard: high stress, moderate, or affordable
    df = df.with_column(
        "STRESS_LABEL",
        when(col("STRESS_PCT") > lit(30), lit("high stress"))       # when(condition, value) likes an if/else. "When this is true, use this value."
        .when(col("STRESS_PCT") > lit(25), lit("moderate"))
        .otherwise(lit("affordable"))
    )
    df.show(10)  # preview only, no write yet

if __name__ == "__main__":
    session = get_session()
    transform_region(session)
    session.close() 
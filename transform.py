import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, round as sp_round, when

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


MEDIAN_INCOME_WEEKLY = 1889

def transform_region(session):
    df = session.table("RENTAL_STRESS.RAW.RAW_REGION")
    df = df.filter(col("TOTAL_MEDIAN").is_not_null())   # col("NAME") means use this column in my calculation
    df = df.filter(col("TOTAL_COUNT").is_not_null())
    df = df.with_column(
        "STRESS_PCT",
        sp_round((col("TOTAL_MEDIAN") / lit(MEDIAN_INCOME_WEEKLY)) * lit(100), 1)  # round as sp_round rounds a number to X decimal places. Renamed to sp_round to avoid clashing with Python's built-in round()
    )
    df = df.with_column(
        "STRESSED",
        when(col("STRESS_PCT") > lit(30), lit(True)).otherwise(lit(False))  # lit(value) wraps a plain Python value (a number, a string) so Snowpark understands it.
    )
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
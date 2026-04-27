import os                        # reads environment variables (like passwords)
import pandas as pd              # reads Excel files and shapes data into tables
import snowflake.connector       # lets Python talk to Snowflake
from dotenv import load_dotenv   # reads a .env file and puts values into environment variables

load_dotenv()  # runs immediately, loads your .env so os.getenv() can find your credentials

# A list of tuples. Each tuple is: (a label you choose, the file path to the Excel file)
QUARTERLY_FILES = [
    ("2025-Q1", "data/private-rental-report-2025-03.xlsx"),
    ("2025-Q2", "data/private-rental-report-2025-06.xlsx"),
    ("2025-Q3", "data/private-rental-report-2025-09.xlsx"),
    ("2025-Q4", "data/private-rental-report-2025-12.xlsx"),
]

# A set of region names to ignore. These are summary rows, not real regions.
SKIP_REGIONS = {"Metro", "Rest of State", "Metro Total", "Rest of State Total", "Grand Total"}

# Hard-coded weekly income used later to calculate rental stress percentage
INCOME_WEEKLY = 1889

# Test: Check if the first quarterly file is correct and if the skip regions set has 5 items
# print(QUARTERLY_FILES[0])  # should print ('2025-Q1', 'data/private-rental-report-2025-03.xlsx')
# print(len(SKIP_REGIONS))   # should print 5


def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),      # reads SNOWFLAKE_ACCOUNT from .env
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

# Test: Check if we can connect to Snowflake using the credentials in .env
# conn = get_connection()
# print("Connected:", conn)       # should print a connection object -> Connected: <snowflake.connector.connection.SnowflakeConnection object at 0x0000027AEB727DA0>
# conn.close()

# Test: Check if we can read the "Region" sheet from the a quarterly file and see the header row
# xl = pd.read_excel("data/private-rental-report-2025-12.xlsx", sheet_name="Region", header=None)
# print(xl.iloc[12])  # prints the header row so you can see each column label e.g. 24: Other/Unknown Median, 25: Total Count, 26: Total Median
# print(xl.iloc[15])  # prints e.g. 0: Metro

def parse_region(quarter, path):
    # reads the "Region" sheet, tells pandas there is no header row
    xl = pd.read_excel(path, sheet_name="Region", header=None)

    # iloc means "select by position"
    # 15: means start at row 15 (skip the first 15 rows, which are header/title rows)
    # [0, 25, 26] means only keep columns 0, 25, and 26 (region name, Total Count, Total Median)
    data = xl.iloc[15:, [0, 25, 26]].copy()
    # Test: Check if we are correctly selecting the region name, total count, and total median columns 
    # print(data)

    # rename those 3 columns to something readable
    data.columns = ["region", "total_count", "total_median"]
    # Test: Check if we have the right column names and data looks correct after renaming   
    # print(data)

    # ~ means "NOT". This removes rows where region name is in SKIP_REGIONS
    data = data[~data["region"].isin(SKIP_REGIONS)]
    # Test: Check if the skip regions have been removed and only real regions remain
    # print(data)

    # to_numeric with errors="coerce" turns non-numbers into NaN
    # notna() keeps only rows where total_median is a real number
    # this removes blank rows or text rows at the bottom of the sheet
    data = data[pd.to_numeric(data["total_median"], errors="coerce").notna()]

    # convert both columns to actual numbers (not strings)
    data["total_median"] = pd.to_numeric(data["total_median"])
    data["total_count"] = pd.to_numeric(data["total_count"], errors="coerce")

    # add a column with the quarter label, e.g. "2025-Q1"
    data["quarter"] = quarter

    # remove leading/trailing spaces from region names
    data["region"] = data["region"].str.strip()
    return data[["quarter", "region", "total_count", "total_median"]]

# Test: Check if parse_region works on the first quarterly file
# df = parse_region("2025-Q4", "data/private-rental-report-2025-12.xlsx")
# print(df.head(12))
# print(df.shape)   # tells you (rows, columns)


def parse_suburb(quarter, path):
    xl = pd.read_excel(path, sheet_name="Suburb", header=None)
    data = xl.iloc[15:, [0, 25, 26]].copy()
    data.columns = ["suburb", "total_count", "total_median"]
    data = data[pd.to_numeric(data["total_median"], errors="coerce").notna()]
    data["total_median"] = pd.to_numeric(data["total_median"])
    data["total_count"] = pd.to_numeric(data["total_count"], errors="coerce")
    data["quarter"] = quarter
    data["suburb"] = data["suburb"].str.strip()
    return data[["quarter", "suburb", "total_count", "total_median"]]

# Test: Check if parse_suburb works on the first quarterly file
# df = parse_suburb("2025-Q4", "data/private-rental-report-2025-12.xlsx")
# print(df.head(10))
# print(df.shape)   # tells you (rows, columns)


def create_tables(cursor):
    # CREATE OR REPLACE means: if the table already exists, delete it and create fresh
    # This is fine for a pipeline that reloads everything each run
    cursor.execute("""
        CREATE OR REPLACE TABLE RENTAL_STRESS.RAW.RAW_REGION (
            quarter       VARCHAR,
            region        VARCHAR,
            total_count   FLOAT,
            total_median  FLOAT
        )
    """)
    cursor.execute("""
        CREATE OR REPLACE TABLE RENTAL_STRESS.RAW.RAW_SUBURB (
            quarter       VARCHAR,
            suburb        VARCHAR,
            total_count   FLOAT,
            total_median  FLOAT
        )
    """)
    print("Tables created.")

# Test: Check if we can create the tables in Snowflake
# in Snowflake that the tables should exist under RENTAL_STRESS > RAW
# conn = get_connection()
# cursor = conn.cursor()
# create_tables(cursor)
# cursor.close()
# conn.close()


def load_dataframe(cursor, df, table):
    # itertuples() loops through each row of the dataframe. itertuples() returns each row as a named tuple, not a plain tuple. A named tuple looks like this: Pandas(quarter='2025-Q1', region='Eastern', total_count=120, total_median=530.0)
    # index=False means don't include the row number
    # tuple(row) converts each row into a plain tuple like ("2025-Q1", "Eastern", 120, 530.0) as The Snowflake connector's executemany expects plain tuples like ('2025-Q1', 'Eastern', 120, 530.0)
    rows = [tuple(row) for row in df.itertuples(index=False)]

    # count how many columns there are (4 in this case)
    cols = len(df.columns)

    # builds "%s, %s, %s, %s" which is one placeholder per column
    # Snowflake uses %s as a safe way to insert values (prevents SQL injection)
    placeholders = ", ".join(["%s"] * cols)

    # executemany sends all rows in one call, more efficient than a loop of execute()
    cursor.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
    print("Data inserted.")

# Test: Check if we can load a dataframe into Snowflake
# pd.DataFrame() can accept a list of tuples as its data. Each tuple becomes one row. The columns argument gives names to each position.
test_df = pd.DataFrame([
    ("2025-Q1", "test", 1000, 1000)
], columns=["quarter", "region", "total_count", "total_median"])
conn = get_connection()
cursor = conn.cursor()
load_dataframe(cursor, test_df, "RENTAL_STRESS.RAW.RAW_REGION")
cursor.close()
conn.close()
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

# Test 1: Check if the first quarterly file is correct and if the skip regions set has 5 items
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

conn = get_connection()
print("Connected:", conn)       # should print a connection object -> Connected: <snowflake.connector.connection.SnowflakeConnection object at 0x0000027AEB727DA0>
conn.close()
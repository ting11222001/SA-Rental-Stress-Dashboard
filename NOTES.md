# NOTES

## Build plan:

```
Here is a step-by-step build plan:

Step 1: Download the data
Go to the SA Government Private Rental Report page and download 4 recent quarterly XLSX files. Save them locally.

Step 2: Set up Snowflake
In your trial account, create a database (e.g. RENTAL_STRESS) and three schemas: RAW, CLEAN, MART.

Step 3: Load raw data (ingest.py)
Write a Python script using the Snowflake Connector or Snowpark to upload the XLSX data into a table in the RAW schema. One row per region per quarter.

Step 4: Transform with Snowpark (transform.py)
Write a Snowpark script that reads from RAW, removes nulls, adds a quarter column, calculates stress_pct = (rent / 1889) * 100, and writes to CLEAN and then MART.

Step 5: Build the Streamlit dashboard
Inside Snowflake, create a Streamlit app that reads from MART and shows: 3 metric cards, a bar chart by region, and a stress table with flags.
```

## Dashboard Design

Section 1 (trend): This needs multiple quarterly files loaded. It requires more ingestion work. This is the hardest part.

Section 2 (most expensive / affordable suburbs): This comes from the Suburb sheet of a single file. Straightforward.

Section 3 (rental stress by region): This comes from the Region sheet. Also straightforward.

### Version 1

If I build the Section 2 and Section 3 first, then I can claim:
```
Data engineering: You are ingesting raw XLSX files, cleaning nulls, reshaping multi-header rows into a flat table, and loading into Snowflake. That is a real ingestion pipeline, not just a copy-paste.

Data modelling: You are designing three layers: raw, clean, and mart. That is a layered data model. The mart table is purpose-built for the dashboard query. This is the same pattern used in professional data warehouses.

Snowflake: All storage and compute runs in Snowflake. You use Snowpark for the transformation, which is Snowflake-native Python.

Python: The ingest script and the Snowpark transform script are both Python.
```

Basically, I want to say: "I built a governed three-layer pipeline on Snowflake using Snowpark for transformation and Streamlit for the serving layer."


### Version 2

I will use 4 quarters 2025 data from Private Rent Report of SA first as ingesting data could take quite some time.

Confirmed scope:
```
Section 1: trend line, 4 quarters 2025, by region, from Region sheet. 
Section 2: top/affordable suburbs, 2025-Q4 only, filtered to 10+ bonds, from Suburb sheet. 
Section 3: rental stress by region, 2025-Q4, hardcoded income $1,889/wk, from Region sheet.
```

## Setup Snowflake environment

Sign up for the trial account which gave me $400 credits and expired in 30 days.

Create a new folder in `My Workspace` called `SA Rental Stress Pipeline for families`.

Create a SQL File called `setup.sql` and add these:
```
CREATE DATABASE RENTAL_STRESS;

CREATE SCHEMA RENTAL_STRESS.RAW;
CREATE SCHEMA RENTAL_STRESS.CLEAN;
CREATE SCHEMA RENTAL_STRESS.MART;
```

Click the run button on the top left.

Then verify it worked:
```
SHOW SCHEMAS IN DATABASE RENTAL_STRESS;
```

The snowflake result:
```
created_on, name, database_name, owner
2026-04-26 19:44:30.313,CLEAN,RENTAL_STRESS,ACCOUNTADMIN
2026-04-26 21:36:26.181,INFORMATION_SCHEMA,RENTAL_STRESS,
2026-04-26 19:44:31.139,MART,RENTAL_STRESS,ACCOUNTADMIN
2026-04-26 19:44:28.665,PUBLIC,RENTAL_STRESS,ACCOUNTADMIN
2026-04-26 19:44:29.497,RAW,RENTAL_STRESS,ACCOUNTADMIN
```

You should see RAW, CLEAN, and MART in the results.

## Setup the ingest script

Find my account identifier on snowflake.

Run this locally to test the `ingest.py`:
```
python ingest.py
```

Output:
```
Parsing 2025-Q1 ...
Region rows: 36
Suburb rows: 2564
Tables created.
Loaded 36 rows into RENTAL_STRESS.RAW.RAW_REGION
Loaded 2564 rows into RENTAL_STRESS.RAW.RAW_SUBURB
Done.
```

### parse_region Test: Check if we can read the "Region" sheet from the a quarterly file and see the header row

```
xl = pd.read_excel("data/private-rental-report-2025-12.xlsx", sheet_name="Region", header=None)

print(xl.iloc[12])

Output:
0                      NaN
1              Flats/Units
2                      NaN
3                      NaN
4                      NaN
5                      NaN
6                      NaN
7                      NaN
8                      NaN
9        Flats/Units Count
10      Flats/Units Median
11                  Houses
12                     NaN
13                     NaN
14                     NaN
15                     NaN
16                     NaN
17                     NaN
18                     NaN
19            Houses Count
20           Houses Median
21           Other/Unknown
22                     NaN
23     Other/Unknown Count
24    Other/Unknown Median
25             Total Count
26            Total Median


print(xl.iloc[15])

Output:
0     Metro
1       NaN
2       NaN
3       NaN
4       NaN
5       NaN
6       NaN
7       NaN
8       NaN
9       NaN
10      NaN
11      NaN
12      NaN
13      NaN
14      NaN
15      NaN
16      NaN
17      NaN
18      NaN
19      NaN
20      NaN
21      NaN
22      NaN
23      NaN
24      NaN
25      NaN
26      NaN
```

### parse_region Test: Check if we are correctly selecting the region name, total count, and total median columns 

```
def parse_region(quarter, path):
    # reads the "Region" sheet, tells pandas there is no header row
    xl = pd.read_excel(path, sheet_name="Region", header=None)

    # iloc means "select by position"
    # 15: means start at row 15 (skip the first 15 rows, which are header/title rows)
    # [0, 25, 26] means only keep columns 0, 25, and 26 (region name, Total Count, Total Median)
    data = xl.iloc[15:, [0, 25, 26]].copy()

    # Test: Check if we are correctly selecting the region name, total count, and total median columns 
    print(data)

parse_region("2025-Q4", "data/private-rental-report-2025-12.xlsx")

Output:
                     0      25   26
15                Metro    NaN  NaN
16    Northern Adelaide   3470  560
17     Western Adelaide   1980  590
18     Eastern Adelaide   2885  575
19    Southern Adelaide   2105  600
20          Metro Total  10440  580
21        Rest of State    NaN  NaN
22       Adelaide Hills    365  600
23      Fleurieu and KI    265  500
24     Eyre and Western    370  360
25            Far North    220  340
26              Barossa    395  550
27    Murray and Mallee    445  420
28  Yorke and Mid North    305  400
29      Limestone Coast    410  400
30  Rest of State Total   2770  450
31          Grand Total  13210  550
```

### par_region Test: Check if parse_region works on the first quarterly file

```
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
df = parse_region("2025-Q4", "data/private-rental-report-2025-12.xlsx")
print(df.head(12))
print(df.shape)   # tells you (rows, columns)

Output:

    quarter               region  total_count  total_median
16  2025-Q4    Northern Adelaide         3470           560
17  2025-Q4     Western Adelaide         1980           590
18  2025-Q4     Eastern Adelaide         2885           575
19  2025-Q4    Southern Adelaide         2105           600
22  2025-Q4       Adelaide Hills          365           600
23  2025-Q4      Fleurieu and KI          265           500
24  2025-Q4     Eyre and Western          370           360
25  2025-Q4            Far North          220           340
26  2025-Q4              Barossa          395           550
27  2025-Q4    Murray and Mallee          445           420
28  2025-Q4  Yorke and Mid North          305           400
29  2025-Q4      Limestone Coast          410           400

Output shape:
(12, 4)
```

### parse_suburb test

```
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
df = parse_suburb("2025-Q4", "data/private-rental-report-2025-12.xlsx")
print(df.head(10))
print(df.shape)   # tells you (rows, columns)


Output:
    quarter           suburb  total_count  total_median
16  2025-Q4   Aberfoyle Park         25.0         630.0
17  2025-Q4         Adelaide       1050.0         514.5
18  2025-Q4      Albert Park         15.0         600.0
19  2025-Q4         Alberton         10.0         520.0
20  2025-Q4          Aldgate          5.0         650.0
21  2025-Q4          Aldinga          NaN         650.0
22  2025-Q4    Aldinga Beach         80.0         577.5
23  2025-Q4  Allenby Gardens         15.0         640.0
24  2025-Q4     Andrews Farm        105.0         550.0
25  2025-Q4       Angle Park          NaN         520.7
26  2025-Q4       Angle Vale         60.0         625.0
27  2025-Q4       Ascot Park         40.0         555.0
(641, 4)
```

### create_tables test

```
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
conn = get_connection()
cursor = conn.cursor()
create_tables(cursor)
cursor.close()
conn.close()
```

Then run this on snowflake:
```
SHOW TABLES IN SCHEMA RENTAL_STRESS.RAW;
```

The snowflake result should be:
```
created_on,name,database_name,schema_name,kind
2026-04-26 21:21:51.730 -0700,RAW_REGION,RENTAL_STRESS,RAW,TABLE
2026-04-26 21:21:51.992 -0700,RAW_SUBURB,RENTAL_STRESS,RAW,TABLE
```

To see the data inside one of them:
```
SELECT * FROM RENTAL_STRESS.RAW.RAW_REGION LIMIT 10;
```

About the naming: 

each entry in a schema is called a table (or object). The full path is always:
```
DATABASE.SCHEMA.TABLE
```

So yours are:
```
RENTAL_STRESS.RAW.RAW_REGION
RENTAL_STRESS.RAW.RAW_SUBURB
```

### load_dataframe test

```
def load_dataframe(conn, df, table):
    # The suburb sheet replaces counts of 1 to 5 dwellings with a * symbol. 
    # When pandas reads that, it cannot convert * to a number, so it stores NaN instead. This meant the data had NaN values that needed to become NULL in Snowflake.
    # write_pandas handles NaN to NULL conversion internally, and it uses a COPY INTO command under the hood instead of row-by-row inserts, so it loads thousands of rows in one operation.
    print(f"Loading data into {table}...")
    df.columns = df.columns.str.upper()
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table,
        database="RENTAL_STRESS",
        schema="RAW",
    )
    print(f"Loaded {nrows} rows. Success: {success}")

# Test: Check if we can load a region dataframe into Snowflake
# pd.DataFrame() can accept a list of tuples as its data. Each tuple becomes one row. The columns argument gives names to each position.
# test_df = pd.DataFrame([
#     ("2025-Q1", "test", 1000, 1000)
# ], columns=["quarter", "region", "total_count", "total_median"])
# conn = get_connection()
# cursor = conn.cursor()
# load_dataframe(cursor, test_df, "RENTAL_STRESS.RAW.RAW_REGION")
# cursor.close()
# conn.close()
```

Run this in a Snowflake worksheet:
```
SELECT * FROM RENTAL_STRESS.RAW.RAW_REGION;
```

The snowflake result:
```
QUARTER,REGION,TOTAL_COUNT,TOTAL_MEDIAN
2025-Q1,test,1000,1000
```

### main function test

```
def main():
    all_region = []   # empty list to collect dataframes from each quarter
    all_suburb = []

    for quarter, path in QUARTERLY_FILES:
        print(f"Parsing {quarter} ...")
        all_region.append(parse_region(quarter, path))   # parse and add to list
        all_suburb.append(parse_suburb(quarter, path))

    # concat stacks all 4 dataframes into one big dataframe
    # ignore_index=True resets the row numbers (0, 1, 2...) on the combined table
    region_df = pd.concat(all_region, ignore_index=True)
    # Test: Check if the combined region dataframe looks correct and has 4 times the rows of one quarter's dataframe
    # print(region_df.head(10))  # check the first 10 rows of the combined region dataframe

    suburb_df = pd.concat(all_suburb, ignore_index=True)
    # Test: Check if the combined suburb dataframe looks correct
    # print(f"Suburb DataFrame (first 10 rows):\n{suburb_df.head(10)}")

    conn = get_connection()
    cursor = conn.cursor()

    create_tables(cursor)   # creates fresh tables
    load_dataframe(conn, region_df, "RAW_REGION")
    load_dataframe(conn, suburb_df, "RAW_SUBURB")

    cursor.close()
    conn.close()
    print("\nDone.")


# this guard means: only run main() if this file is run directly
# if another file imports this script, main() won't run automatically
if __name__ == "__main__":
    main()
```

Final test: Once all 4 Excel files are in data/, run the full script:
```
python ingest.py
```

Terminal prints these output:
```
Parsing 2025-Q1 ...
Parsing 2025-Q2 ...
Parsing 2025-Q3 ...
Parsing 2025-Q4 ...
   quarter             region  total_count  total_median
0  2025-Q1  Northern Adelaide         3445           550
1  2025-Q1   Western Adelaide         2015           570
2  2025-Q1   Eastern Adelaide         4905           455
3  2025-Q1  Southern Adelaide         2320           570
4  2025-Q1     Adelaide Hills          335           590
5  2025-Q1    Fleurieu and KI          245           490
6  2025-Q1   Eyre and Western          455           350
7  2025-Q1          Far North          220           330
8  2025-Q1            Barossa          380           525
9  2025-Q1  Murray and Mallee          400           405


   quarter           suburb  total_count  total_median
0  2025-Q1   Aberfoyle Park         35.0         580.0
1  2025-Q1         Adelaide       2945.0         409.0
2  2025-Q1      Albert Park         10.0         685.0
3  2025-Q1         Alberton         15.0         450.0
4  2025-Q1          Aldgate          NaN        1200.0
5  2025-Q1          Aldinga          NaN         460.0
6  2025-Q1    Aldinga Beach         70.0         565.0
7  2025-Q1  Allenby Gardens          NaN         720.0
8  2025-Q1     Andrews Farm        110.0         540.0
9  2025-Q1       Angle Park         25.0         600.0
```

Then verify in Snowflake:
```
SELECT COUNT(*) FROM RENTAL_STRESS.RAW.RAW_REGION;
SELECT COUNT(*) FROM RENTAL_STRESS.RAW.RAW_SUBURB;
```
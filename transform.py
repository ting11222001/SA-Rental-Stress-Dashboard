import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

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

if __name__ == "__main__":
    session = get_session()
    print("Connected:", session.get_current_database())
    session.close()
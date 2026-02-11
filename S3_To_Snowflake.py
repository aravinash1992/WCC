#pip install snowflake-connector-python boto3 python-dotenv
#pip install snowflake-connector-python
#python -m pip install snowflake-connector-python
#pip install --upgrade pip setuptools wheel


import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# ---------- Snowflake Config ----------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

cs = conn.cursor()

try:
    # 1️⃣ File Format
    cs.execute("""
        CREATE OR REPLACE FILE FORMAT CSV_FMT
        TYPE = CSV
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL','null','')
    """)

    # 2️⃣ External Stage
    cs.execute(f"""
        CREATE OR REPLACE STAGE S3_STAGE
        URL = 's3://calibo-dia-tma/Source/'
        CREDENTIALS = (
            AWS_KEY_ID = '{os.getenv("AWS_ACCESS_KEY_ID")}'
            AWS_SECRET_KEY = '{os.getenv("AWS_SECRET_ACCESS_KEY")}'
        )
        FILE_FORMAT = CSV_FMT
    """)

    # 3️⃣ Auto-create Table
    cs.execute("""
        CREATE OR REPLACE TABLE VOLUME
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '@S3_STAGE/VOLUME.csv',
                    FILE_FORMAT => 'CSV_FMT'
                )
            )
        )
    """)

    # 4️⃣ Load Data
    cs.execute("""
        COPY INTO VOLUME
        FROM @S3_STAGE/VOLUME.csv
        FILE_FORMAT = (FORMAT_NAME = 'CSV_FMT')
        ON_ERROR = 'ABORT_STATEMENT'
    """)

    print("✅ Data loaded into Snowflake successfully")

finally:
    cs.close()
    conn.close()

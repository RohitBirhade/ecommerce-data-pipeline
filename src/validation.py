import pandas as pd
from pyspark.sql import SparkSession
import config


def validate_source_schema(df):
    result = {
        "expected" : config.data_contract["source_columns"],
        "actual" : None,
        "status" : "",
        "level"  : "",
        "message": ""
    }
    try:
        result["actual"] = df.columns.tolist()
        file_columns = df.columns.tolist()
        file_columns = [column.lower() for column in file_columns]
        if file_columns == config.data_contract["source_columns"]:
            result["status"] = "PASS"
            result["level"] = "NORMAL"
            result["message"] = "Source columns match data contract"
        else:
            result["status"] = "FAIL"
            result["level"] = "CRITICAL"
            result["message"] = "Source columns doesn't match data contract"
    except Exception as e:
        result["status"] = "FAIL"
        result["level"] = "ERROR"
        result["message"] = str(e)
    
    return result



def table_exists(conn):
    result = {
        "expected" : "raw.online_retail",
        "actual" : None,
        "status" : "",
        "level"  : "",
        "message": ""
    }
    isTable_Exists = '''select * 
        from information_schema.tables
        where table_schema = 'raw' and table_name = 'online_retail';
    '''
    try:
        with conn.cursor() as cur:
            
            cur.execute(isTable_Exists)
            response = cur.fetchone()
            if response:
                tableName = f"{response[1]}.{response[2]}"
                result["actual"] = tableName        
                if tableName == result["expected"]:
                    result["status"] = "PASS"
                    result["level"] = "NORMAL"
                    result["message"] = "Table is available"
            else:
                result["status"] = "FAIL"
                result["level"] = "CRITICAL"
                result["message"] = "Table doesn't exists"
    except Exception as e:
        result["status"] = "FAIL"
        result["level"] = "ERROR"
        result["message"] = str(e)
    
    return result



def validate_table_schema(conn):
    result = {
        "expected": config.data_contract["target_schema"],
        "actual": None,
        "status": "",
        "level": "",
        "message": ""
    }

    query = """
        SELECT column_name, data_type, is_nullable,
               numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name = 'online_retail'
        ORDER BY ordinal_position;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

            actual_schema = [
                {
                    "column_name" : row[0],
                    "data_type" : row[1],
                    "is_nullable" : row[2],
                    "numeric_precision" : row[3],
                    "numeric_scale" : row[4]
                }

                for row in rows
            ]

            result["actual"] = actual_schema
            if actual_schema == result["expected"]:
                result["status"] = "PASS"
                result["level"] = "NORMAL"
                result["message"] = "Table schema matches data contract"
            else:
                result["status"] = "FAIL"
                result["level"] = "CRITICAL"
                result["message"] = "Table schema doesn't match data contract"

    except Exception as e:
        result["status"] = "FAIL"
        result["level"] = "ERROR"
        result["message"] = str(e)

    return result


def validate_table_data(df, conn):

    result = {
        "expected": None,
        "actual": None,
        "status": "",
        "level": "",
        "message": ""
    }

    na_count_query = """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE description IS NULL) AS missing_descriptions,
            COUNT(*) FILTER (WHERE customerid IS NULL) AS missing_customer_ids,
            COUNT(*) FILTER (WHERE invoicedate IS NULL) AS missing_invoice_dates
        FROM raw.online_retail;
    """

    dup_count_query = """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM (
            SELECT COUNT(*) AS duplicate_count
            FROM raw.online_retail
            GROUP BY invoiceno, stockcode, description, quantity,
                     invoicedate, unitprice, customerid, country
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """

    try:
        df_na = df.isnull().sum()

        result["expected"] = (
            len(df),
            int(df_na["Description"]),
            int(df_na["CustomerID"]),
            int(df_na["InvoiceDate"]),
            int(df.duplicated().sum())
        )

        with conn.cursor() as cur:
            cur.execute(na_count_query)
            na_res = tuple(int(x) for x in cur.fetchone())

            cur.execute(dup_count_query)
            dup_res = int(cur.fetchone()[0])

        result["actual"] = na_res + (dup_res,)

        if result["expected"] == result["actual"]:
            result["status"] = "PASS"
            result["level"] = "NORMAL"
            result["message"] = "Raw table data matches source data"
        else:
            result["status"] = "FAIL"
            result["level"] = "WARN"
            result["message"] = "Raw table data does not match source data"

    except Exception as e:
        result["status"] = "FAIL"
        result["level"] = "ERROR"
        result["message"] = str(e)

    return result
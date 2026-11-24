import pandas as pd
from pyspark.sql import SparkSession
import config
import os


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


def validate_export_csv(rows_count, conn):
    
    result = {
        "expected": None,
        "actual": rows_count,
        "status": "",
        "level": "",
        "message": ""
    }

    query = '''select count(*)
            from raw.online_retail'''
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result['expected'] = cur.fetchone()[0]
            if result['expected'] == result['actual']:
                result['status'] = "PASS"
                result['level'] = 'NORMAL'
                result['message'] = 'The csv row count matches table count'
            else:
                result['status'] = "FAIL"
                result['level'] = 'CRITICAL'
                result['message'] = '''The csv row count doesn't match table count'''

    except Exception as e:
        result['status'] = 'FAIL'
        result['level'] = 'ERROR'
        result['message'] = str(e)

    return result

def validate_upload_to_s3(output_path, response):
    
    result = {
            "expected": None,
            "actual": response['ContentLength'],
            "status": "",
            "level": "",
            "message": ""
    }

    try:
        local_size = os.path.getsize(output_path)
        result['expected'] = local_size
        if result['expected'] == result['actual']:
            result['status'] = 'PASS'
            result['level'] = 'NORMAL'
            result['message'] = 'The uploaded file size matches local'
        else:
            result['status'] = 'FAIL'
            result['level'] = 'CRITICAL'
            result['message'] = '''The uploaded file size doesn't match local'''
    except Exception as e:
        result['status'] = 'FAIL'
        result['level'] = 'ERROR'
        result['message'] = str(e)

    return result


def validate_sparkSession(spark):
    
    result = {
            "status": "",
            "level": "",
            "message": ""
        }
    try:
        if spark:
            result['status'] = 'PASS'
            result['level'] = 'NORMAL'
            result['message'] = 'session exists'
        else:
            result['status'] = 'FAIL'
            result['level'] = 'CRITICAL'
            result['message'] = '''session doesn't exists'''
    except Exception as e:
        result['status'] = 'FAIL'
        result['level'] = 'ERROR'
        result['message'] = str(e)
    
    return result

def validate_df(df):

    result = {
            "expected": 8,
            "actual": None,
            "status": "",
            "level": "",
            "message": ""
        }

    try:
        result['actual'] = len(df.columns)
        
        if result["actual"] == result["expected"] and df.count() > 0:
            result['status'] = 'PASS'
            result['level'] = 'NORMAL'
            result['message'] = 'session exists'
        else:
            result['status'] = 'FAIL'
            result['level'] = 'CRITICAL'
            result['message'] = '''session doesn't exists'''
    except Exception as e:
        result['status'] = 'FAIL'
        result['level'] = 'ERROR'
        result['message'] = str(e)   
    
    return result     

def validate_df_transformed(df):

    result = {
            "expected": (0,0,0,0,0,0),
            "actual": None,
            "status": "",
            "level": "",
            "message": ""
        }
    
    try:
        if df.count() > 0:
            duplicate_count = df.count() - df.distinct().count()
            cancellation_rec = df.filter("invoiceno LIKE '%C' or invoiceno LIKE 'c%'").count()
            below_zero_unit = df.filter("unitprice <= 0").count()
            below_zero_total = df.filter("quantity <= 0").count()
            zero_rev_count = df.filter("revenue == 0").count()
            neg_rev_count = df.filter("revenue < 0").count()
            result['actual'] = (duplicate_count, cancellation_rec, below_zero_total, below_zero_unit, zero_rev_count, neg_rev_count)
            
            if result['expected'] == result['actual']:
                result['status'] = 'PASS'
                result['level'] = 'NORMAL'
                result['message'] = 'validation successfull' 
            else:
                result['status'] = 'FAIL'
                result['level'] = 'CRITICAL'
                result['message'] = '''validation failed'''
        else:
            result['status'] = 'FAIL'
            result['level'] = 'CRITICAL'
            result['message'] = '''data frame is empty'''
    except Exception as e:
        result['status'] = 'FAIL'
        result['level'] = 'ERROR'
        result['message'] = str(e)

    return result

def validate_analytics_table(conn):
    result = {
        "expected" : "analytics.online_retail",
        "actual" : None,
        "status" : "",
        "level"  : "",
        "message": ""
    }
    isTable_Exists = '''select * 
        from information_schema.tables
        where table_schema = 'analytics' and table_name = 'online_retail';
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
                    result["message"] = "analytics.online_retail table is available"
            else:
                result["status"] = "FAIL"
                result["level"] = "CRITICAL"
                result["message"] = "analytics.online_retail doesn't exists"
    except Exception as e:
        result["status"] = "FAIL"
        result["level"] = "ERROR"
        result["message"] = str(e)
    
    return result

def validate_analytics_data(input_file, conn):

    result = {
        "expected": None,
        "actual": None,
        "status": "",
        "level": "",
        "message": ""
    }

    query = "SELECT COUNT(*) FROM analytics.online_retail;"

    try:
        with open(input_file, "r", encoding="utf-8") as f:
            expected_count = sum(1 for _ in f) - 1  # Exclude header

        with conn.cursor() as cur:
            cur.execute(query)
            actual_count = cur.fetchone()[0]

        result["expected"] = expected_count
        result["actual"] = actual_count

        if expected_count == actual_count:
            result["status"] = "PASS"
            result["level"] = "NORMAL"
            result["message"] = "Analytics table row count matches transformed CSV"
        else:
            result["status"] = "FAIL"
            result["level"] = "CRITICAL"
            result["message"] = "Analytics table row count does not match transformed CSV"

    except Exception as e:
        result["status"] = "FAIL"
        result["level"] = "ERROR"
        result["message"] = str(e)

    return result

def validate_download_from_s3(local_path, response):

    try:
        local_size = os.path.getsize(local_path)
        s3_size = response['ContentLength']

        result = {
            "expected": s3_size,
            "actual": local_size,
            "status": "PASS",
            "level": "NORMAL",
            "message": "S3 download validated successfully"
        }

        if local_size != s3_size:
            result.update({
                "status": "FAIL",
                "level": "CRITICAL",
                "message": "Downloaded file size does not match S3 object size"
            })

        return result

    except Exception as e:
        return {
            "expected": None,
            "actual": None,
            "status": "FAIL",
            "level": "CRITICAL",
            "message": f"Error validating S3 download: {e}"
        }
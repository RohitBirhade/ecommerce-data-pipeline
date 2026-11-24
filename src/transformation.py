from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import boto3
from botocore.exceptions import ClientError, NoCredentialsError



def download_from_s3(local_path, bucket, s3_key):
    
    try:
        s3 = boto3.client('s3')
        s3.download_file(Bucket=bucket, Key=s3_key, Filename=local_path)
        return s3.head_object(Bucket=bucket, Key=s3_key)
    
    except ClientError:
        raise
    except NoCredentialsError:
        raise
    except Exception as e:
        raise
        

def create_spark_session():
    return(
        SparkSession.builder
        .appName('ecommerce_data_pipeline')
        .master('local[*]')
        .getOrCreate()
    )

def read_source_data(spark, source_file):
    return(
        spark.read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv(source_file)
    )

def apply_transformation(df, destination_path):
    try:
        # Removing duplicates
        df_deduplicated = df.distinct()

        # Cleaning cancelled invoices
        df_clean_invoice = df_deduplicated.filter("invoiceno NOT LIKE 'C%' and invoiceno NOT LIKE 'c%'")

        # Removing quantities with <=0 value
        df_clean_quantity = df_clean_invoice.filter("quantity > 0")

        # Removing unitprice with <=0 value
        df_clean_unitprice = df_clean_quantity.filter("unitprice > 0")

        # Adding revenue column
        df_transformed = df_clean_unitprice.withColumn("revenue", (df_clean_unitprice.quantity * df_clean_unitprice.unitprice))

        # Since df_transformed.write.mode("overwrite") doesn't work
        pd_df = df_transformed.toPandas()
        pd_df.to_csv(destination_path, index=False, na_rep=r'\N')

        return df_transformed

    except Exception as e:
        print(f"Error: {e}")
        raise


def create_analytics_table(conn):

    table_query = '''CREATE TABLE IF NOT EXISTS analytics.online_retail(
        invoiceno TEXT,
        StockCode TEXT,
        Description TEXT,
        Quantity INT,
        InvoiceDate TIMESTAMP,
        UnitPrice Decimal(7,2),
        CustomerID TEXT,
        Country TEXT,
        Revenue Decimal(8,2)
    )
    '''

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        cur.execute(table_query)
        conn.commit()

def load_data_to_analytics(conn, input_file):

    load_sql = ''' 
                COPY analytics.online_retail(
                invoiceno,
                stockcode,
                description,
                quantity,
                invoicedate,
                unitprice,
                customerid,
                country,
                revenue
                )
                FROM STDIN
                WITH (FORMAT CSV, HEADER, NULL '\\N')
            '''
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE analytics.online_retail")
        try:
            with open(input_file, 'rb') as f:
                cur.copy_expert(load_sql, f)
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"ERROR: {e}")
            raise


if __name__ == "__main__":

    try:
        pass
    except Exception as e:
        raise e
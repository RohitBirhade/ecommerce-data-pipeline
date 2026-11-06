from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import os
import pandas
from psycopg2 import connect

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

def data_profilling(df):
    print('Data Profiling with PySpark:')
    print(f"Total Rows Count: {df.count()}")
    duplicate_records = ( df
        .groupBy(*df.columns)
        .count()
        .filter('count > 1')
        .agg(expr('sum(count - 1) as Duplicate_records'))
    )
    print("Duplicate count:")
    duplicate_records.show()
    # print(f"Duplicate Count: {df.count() - df.distinct().count()}")
    print(f"Quantity with 0 or less count: {df.filter('quantity <= 0').count()}")
    print(f"Unitprice with 0 or less count: {df.filter('unitprice <= 0').count()}")
    print(f"Missing customerid count: {df.filter('customerid is NULL').count()}")
    print(f"Missing description count: {df.filter('description is NULL').count()}")
    print("Cancellation invoices count: ", (df.filter("invoiceno LIKE 'C%' or invoiceno LIKE 'c%'").count()))


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

        df_transformed.show(5)
        
        # Validation
        # print("Total Records:", df_transformed.count())
        # print("Duplicate Records: ", df_transformed.count() - df_transformed.distinct().count())
        # print("Total Cancellation Records:", df_transformed.filter("invoiceno LIKE '%C' or invoiceno LIKE 'c%'").count())
        # print("Total Unit Price <= 0:", df_transformed.filter("unitprice <= 0").count())
        # print("Total quantity <= 0:", df_transformed.filter("quantity <= 0").count())
        # print("Missing CustomerID count:", df_transformed.filter("customerid is NULL").count())
        # print("Missing Discription count:", df_transformed.filter("description is NULL").count())
        # print("Revenue values that equal to 0: ", df_transformed.filter("revenue == 0").count())
        # print("Negative Revenue values: ", df_transformed.filter("revenue < 0").count())

        # print("Missing Description count: ",df_deduplicated.filter("description is NULL").count())
        # print("Missing Description count: ",df_clean_quantity.filter("description is NULL").count())
        # print("Missing Discription count:", df_clean_unitprice.filter("description is NULL").count())
        # print(os.getenv("HADOOP_HOME"))
        
        # Since df_transformed.write.mode("overwrite") doesn't work
        pd_df = df_transformed.toPandas()
        pd_df.to_csv(destination_path, index=False, na_rep=r'\N')

    except Exception as e:
        print(f"Error: {e}")
        raise

def db_conn():
    config = {
        "host":os.getenv("pssql_host"),
        "database":os.getenv("pssql_database"),
        "user":os.getenv("pssql_user"),
        "password":os.getenv("pssql_user_pass")
    }
    return connect(**config)

def create_table(conn):

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
        # cur.execute("DROP TABLE IF EXISTS analytics.online_retail")
        cur.execute(table_query)
        conn.commit()
        print("Table Created Succefully")

def load_data(conn, destination_file):

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
            with open(destination_file, 'rb') as f:
                cur.copy_expert(load_sql, f)
                conn.commit()
                print(f"Data Loaded Successfully")
        except Exception as e:
            conn.rollback()
            print(f"ERROR: {e}")
            raise


if __name__ == "__main__":

    try:
        local_file = r"D:\Develop\BigData\Projects\ecommerce-data-pipeline\data\load\online_retail.csv"
        destination_file = r"D:\Develop\BigData\Projects\ecommerce-data-pipeline\data\transformed\online_retail_processed.csv"
        # spark = create_spark_session()
        # df = read_source_data(spark=spark, source_file=local_file)
        # data_profilling(df)
        # apply_transformation(df, destination_path=destination_file)
        conn = db_conn()
        create_table(conn)
        load_data(conn, destination_file=destination_file)

    except Exception as e:
        raise e
    finally:
        # spark.stop()
        conn.close()
        pass
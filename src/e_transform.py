from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

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


if __name__ == "__main__":

    try:
        local_file = r"D:\Develop\BigData\Projects\ecommerce-data-pipeline\data\online_retail.csv"
        spark = create_spark_session()
        df = read_source_data(spark=spark, source_file=local_file)
        data_profilling(df)

        
    except Exception as e:
        raise e
    finally:
        spark.stop()
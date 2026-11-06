import os
import csv
import boto3
from botocore import exceptions
from psycopg2 import connect



def export_to_csv(output_path, batch_size=10000):

    query = '''SELECT
                    invoiceno,
                    stockcode,
                    description,
                    quantity,
                    invoicedate,
                    unitprice,
                    customerid,
                    country
            FROM raw.online_retail;'''
    try:
        config ={
            "host": os.getenv("pssql_host"),
            "user": os.getenv("pssql_user"),
            "password": os.getenv("pssql_user_pass"),
            "database": os.getenv("pssql_database")
        }
        with connect(**config) as conn:
            with conn.cursor("online_retail") as cur:

                cur.execute(query)
                total_rows = 0
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    
                    file_exists = os.path.exists(output_path)
                    is_empty = (os.path.getsize(output_path) == 0) if file_exists else True
                    headers = ["invoiceno","stockcode","description","quantity","invoicedate","unitprice","customerid","country"]
                    csv_writer = csv.writer(f, headers)
                    
                    if is_empty:
                        csv_writer.writerow(headers)
                    while True:
                        rows = cur.fetchmany(batch_size)
                        if not rows:
                            break
                        csv_writer.writerows(rows)
                        total_rows += len(rows)
                    print(f'Total Rows written in file: {total_rows}')

    except Exception as e:
        print(f"Error: {e}")


def upload_to_s3(output_path, bucket, s3_key):

    if not os.path.isfile(output_path):
        raise FileNotFoundError(output_path)

    s3 = boto3.client("s3")
    sts = boto3.client("sts")

    identity = sts.get_caller_identity()

    print(f"Account: {identity['Account']}")
    print(f"ARN: {identity['Arn']}")

    local_size = os.path.getsize(output_path)

    s3.upload_file(
        output_path,
        bucket,
        s3_key
    )

    response = s3.head_object(
        Bucket=bucket,
        Key=s3_key
    )

    uploaded_size = response["ContentLength"]

    print(f"Content Type: {response.get('ContentType')}")
    print(f"Local file size: {local_size / 1024 / 1024:.2f} MB")
    print(f"S3 file size: {uploaded_size / 1024 / 1024:.2f} MB")
    print(f"Size matches: {local_size == uploaded_size}")

    if local_size != uploaded_size:
        raise ValueError("Uploaded file size doesn't match local file size")

    print("Upload and size validation successful!")


if __name__ == "__main__":
    
    output_path = r"D:\Develop\BigData\Projects\ecommerce-data-pipeline\data\load\online_retail.csv"
    bucket = "ecommerce-pipeline-raw-data-pipeline"
    s3_key = 'raw/online_retail/online_retail.csv'
    # export_to_csv(output_path=output_path)
    upload_to_s3(output_path=output_path, bucket=bucket, s3_key=s3_key)
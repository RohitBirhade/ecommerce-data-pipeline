import os
import csv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError



def export_to_csv(conn, output_path, batch_size=10000):

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
        with conn.cursor("online_retail") as cur:

            cur.execute(query)
            total_rows = 0
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                
                is_empty = (os.path.getsize(output_path) == 0)
                headers = ["invoiceno","stockcode","description","quantity","invoicedate","unitprice","customerid","country"]
                csv_writer = csv.writer(f)
                
                if is_empty:
                    csv_writer.writerow(headers)
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    csv_writer.writerows(rows)
                    total_rows += len(rows)
        return total_rows
    except Exception as e:
        print(f"Error: {e}")


def upload_to_s3(output_path, bucket, s3_key):

    try:
        if not os.path.isfile(output_path):
            raise FileNotFoundError(output_path)

        s3 = boto3.client("s3")
        s3.upload_file(
            output_path,
            bucket,
            s3_key
        )

        response = s3.head_object(
            Bucket=bucket,
            Key=s3_key
        )

        return response
    
    except ClientError:
        raise
    except NoCredentialsError:
        raise
    except Exception as e:
        raise

if __name__ == "__main__":
    pass
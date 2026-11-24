import config
from database import create_conn
from ingestion import read_file, create_table, load_data_to_raw
from validation import *
from loading import export_to_csv, upload_to_s3
from transformation import *


def check_validation(result, level):
    
    if result["status"] != "PASS":
        print(f"\nPIPELINE FAILED at: {level}")
        print(f"Severity: {result['level']}")
        print(f"Reason: {result['message']}")
        raise RuntimeError(f"Pipeline stopped at {level}")


if __name__ == "__main__":
    
    db_conn = None
    spark_obj = None
    try:
        level = 'DB'
        db_conn = create_conn(config.db_config)

        # Ingestion ---------------------------------------------------------------

        level = 'Ingestion'
        df = read_file(filepath=config.raw_path)
        result = validate_source_schema(df)
        check_validation(result=result, level=level)
        
        create_table(db_conn)
        result = table_exists(db_conn)
        check_validation(result=result, level=level)

        result = validate_table_schema(db_conn)
        check_validation(result=result, level=level)
        
        load_data_to_raw(db_conn, df)
        result = validate_table_data(df, db_conn)
        check_validation(result=result, level=level)
        print("Ingestion Stage Complete")


        # Loading ------------------------------------------------------------------
        
        level = 'Load'
        rows_written = export_to_csv(db_conn, output_path=config.staged_path)
        result = validate_export_csv(rows_written, db_conn)
        check_validation(result=result, level=level)

        response = upload_to_s3(output_path=config.staged_path, bucket=config.bucket, s3_key=config.s3_key)
        validate_upload_to_s3(output_path=config.staged_path, response=response)
        check_validation(result=result, level=level)
        print("Loading Stage Complete")

        # Transformation -----------------------------------------------------------

        level = 'Transform'

        response = download_from_s3(local_path=config.cloud_file_path, bucket=config.bucket, s3_key=config.s3_key)
        result = validate_download_from_s3(local_path=config.cloud_file_path, response=response)
        check_validation(result=result, level=level)

        spark_obj = create_spark_session()
        response = validate_sparkSession(spark=spark_obj)
        check_validation(result=result, level=level)

        df = read_source_data(spark=spark_obj, source_file=config.cloud_file_path)
        response = validate_df(df=df)
        check_validation(result=result, level=level)

        df_transformed = apply_transformation(df=df, destination_path=config.transformed_path)
        result = validate_df_transformed(df=df_transformed)
        check_validation(result=result, level=level)

        create_analytics_table(conn=db_conn)
        result = validate_analytics_table(conn=db_conn)
        check_validation(result=result, level=level)

        load_data_to_analytics(conn=db_conn, input_file=config.transformed_path)
        result = validate_analytics_data(conn=db_conn, input_file=config.transformed_path)
        check_validation(result=result, level=level)
        print("Transformation Stage Complete")


    except Exception as e:
        print(f"ERROR in step {level}")
        raise e
    finally:
        if db_conn is not None:
            db_conn.close()
        if spark_obj is not None:
            spark_obj.stop()
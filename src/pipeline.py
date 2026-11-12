import config
from database import create_conn
from ingestion import read_file, create_table, load_data
from validation import validate_source_schema, table_exists, validate_table_schema, validate_table_data

if __name__ == "__main__":
    
    try:
        
        db_conn = create_conn(config.db_config)

        # Ingestion
        df = read_file(filepath=config.raw_path)
        result = validate_source_schema(df)
        create_table(db_conn)
        result = table_exists(db_conn)
        result = validate_table_schema(db_conn)
        load_data(db_conn)
        result = validate_table_data(df, db_conn)


    except:
        pass
    finally:
        db_conn.close()
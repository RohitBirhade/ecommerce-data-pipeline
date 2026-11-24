from contextlib import closing, contextmanager
import pandas as pd
import io


def create_table(conn):

    create_table = '''CREATE TABLE IF NOT EXISTS raw.online_retail( 
        InvoiceNo TEXT, \
        StockCode TEXT, \
        Description TEXT,
        Quantity INT,
        InvoiceDate TIMESTAMP,
        UnitPrice Decimal(7,2),
        CustomerID TEXT,
        Country TEXT
    )'''

    with closing(conn.cursor()) as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
        cur.execute(create_table)
        conn.commit()


def read_file(filepath):

    df = pd.read_excel(filepath)
    
    # Normalize CustomerID to nullable string
    df["CustomerID"] = df["CustomerID"].astype("Int64").astype("string")
    
    return df


def load_data_to_raw(conn, df):
    
    try:
        with conn.cursor() as cur:

            # if function ran twice, we don't want duplicates getting inserted
            # also inside try i.e., if load failed then rollback will undo the changes including truncate
            cur.execute("TRUNCATE TABLE raw.online_retail")  

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep=r'\N')
        
            buffer.seek(0)
            cur.copy_expert("""
            COPY raw.online_retail (
                invoiceno,
                stockcode,
                description,
                quantity,
                invoicedate,
                unitprice,
                customerid,
                country
            )
            FROM STDIN
            WITH (FORMAT CSV, NULL '\\N')
            """, buffer)
            conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Load failed: {e}")
        raise


if __name__ == "__main__":
    
    # conn =  db .... 
    try:
        create_table(conn="")
    finally:
        # conn.close()
        pass
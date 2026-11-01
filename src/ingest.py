from contextlib import closing, contextmanager
import pandas as pd
import psycopg2
import io
import os


def db_conn():

    conn = psycopg2.connect(
        database = os.environ.get("pssql_database"),
        user = os.environ.get("pssql_user"), 
        password = os.environ.get("pssql_user_pass"),
        host = os.environ.get("pssql_host"),
    )
 
    return conn

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
        cur.execute(create_table)
        conn.commit()


def table_exists(conn):
    
    isTable_Exists = '''select * 
        from information_schema.tables
        where table_schema = 'raw' and table_name = 'online_retail'
    '''
    with conn.cursor() as cur:
        cur.execute(isTable_Exists)
        print(cur.fetchall())

def verify_table(conn):

    verTable_Struct = '''select column_name, data_type, is_nullable, numeric_precision, numeric_scale
        from information_schema.columns
        where table_schema = 'raw' and table_name = 'online_retail'
        order by ordinal_position
    '''
    with closing(conn.cursor()) as cur:
        cur.execute(verTable_Struct)
        print(cur.fetchall())

# pandas has built-in contextmanager! no need to worry
def read_file(filepath=r'data/online retail/Online Retail.xlsx'):

    df = pd.read_excel(filepath)
    # Normalize CustomerID to nullable string
    df["CustomerID"] = df["CustomerID"].astype("Int64").astype("string")
    # print(df.isnull().sum())
    # print(df.dtypes)
    # print(df.shape)
    return df


def load_data(conn, df):
    cur = None
    try:
        cur = conn.cursor() 
        cur.execute("TRUNCATE TABLE raw.online_retail")  # if function ran twice, we don't want duplicates getting inserted

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
    finally:
        if cur is not None:
            cur.close()

def validate_table_data(conn):

    with conn.cursor() as cur:
        cur.execute('''SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE description IS NULL) AS missing_descriptions,
            COUNT(*) FILTER (WHERE customerid IS NULL) AS missing_customer_ids,
            COUNT(*) FILTER (WHERE invoicedate IS NULL) AS missing_invoice_dates
            FROM raw.online_retail
        ''')
        print(f'Missing Data check: \n{cur.fetchone()}')

        cur.execute('''SELECT customerid
            FROM raw.online_retail
            WHERE customerid IS NOT NULL
            LIMIT 10;
        ''')
        print(f'CustomerId check: \n{cur.fetchall()}')

        cur.execute('''SELECT SUM(duplicate_count - 1)
            FROM (
                SELECT COUNT(*) AS duplicate_count
                FROM raw.online_retail
                GROUP BY invoiceno, stockcode, description, quantity,
                        invoicedate, unitprice, customerid, country
                HAVING COUNT(*) > 1
            ) AS duplicates;
        ''')
        print(f'Duplicate check: \n{cur.fetchone()}')



if __name__ == "__main__":
    
    conn = db_conn()
    try:
        # create_table(conn)
        # table_exists(conn)
        # verify_table(conn)

        # df = read_file()
        # load_data(conn, df)
        validate_table_data(conn)

    finally:
        conn.close()
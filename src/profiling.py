import pandas as pd
from pyspark.sql.functions import *

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


filepath = r'data/online retail/Online Retail.xlsx'
df = pd.read_excel(filepath)

print(f'Shape: {df.shape}')
print(f'Columns Names: {df.columns.tolist()}')
print(f'Data Types: {df.dtypes}')

print('\nFirst 5 rows:')
print(df.head())

# print(f'\nMissing Values: {df.isnull().sum()}')
# print(f'Duplicate Values: {df.duplicated().sum()}')

# print("\nQuantity summary:")
# print(df["Quantity"].describe())

# print("\nUnit price summary:")
# print(df["UnitPrice"].describe())

# print("\nInvoice date range:")
# print(df["InvoiceDate"].min(), "to", df["InvoiceDate"].max())

# print("\n--- Quantity and price anomalies ---")

# print("Quantity <= 0:")
# print((df["Quantity"] <= 0).sum())

# print("UnitPrice < 0:")
# print((df["UnitPrice"] < 0).sum())

# print("UnitPrice == 0:")
# print((df["UnitPrice"] == 0).sum())

# print("\n--- Invoice patterns ---")

# print("Invoices starting with C:")
# print(
#     df["InvoiceNo"]
#     .astype(str)
#     .str.startswith("C")
#     .sum()
# )

# print("\n--- Missing customer IDs by country ---")

# print(
#     df.assign(
#         MissingCustomer=df["CustomerID"].isna()
#     )
#     .groupby("Country")["MissingCustomer"]
#     .agg(["sum", "count", "mean"])
#     .sort_values("sum", ascending=False)
#     .head(10)
# )

# print("\n--- Largest absolute quantities ---")

# print(
#     df.loc[
#         df["Quantity"].abs().nlargest(10).index,
#         ["InvoiceNo", "StockCode", "Quantity",
#          "UnitPrice", "CustomerID"]
#     ]
# )

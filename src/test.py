import pandas as pd
import re


location = r'data/online retail/Online Retail.xlsx'
df = pd.read_excel(location)


print('InvoiceNo and StockCode')
print(df[['InvoiceNo', 'StockCode']].head())
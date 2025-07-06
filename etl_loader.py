import pandas as pd
import os
import re
from sqlalchemy import create_engine


username = 'root'
password = 'root123'
server = 'localhost'
database = 'celebalassignment'
driver = 'ODBC Driver 17 for SQL Server'


connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
engine = create_engine(connection_string)


folder_path = 'C:/Users/Sanyam/assignment7/'

for file in os.listdir(folder_path):
    if file.startswith('CUST_MSTR'):
        match = re.search(r'CUST_MSTR_(\d{8})\.csv', file)
        if match:
            date_str = match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            df = pd.read_csv(os.path.join(folder_path, file))
            df['date'] = formatted_date
            engine.execute("TRUNCATE TABLE CUST_MSTR")
            df.to_sql('CUST_MSTR', engine, if_exists='append', index=False)


for file in os.listdir(folder_path):
    if file.startswith('master_child_export'):
        match = re.search(r'master_child_export-(\d{8})\.csv', file)
        if match:
            date_str = match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            df = pd.read_csv(os.path.join(folder_path, file))
            df['date'] = formatted_date
            df['datekey'] = date_str
            engine.execute("TRUNCATE TABLE master_child")
            df.to_sql('master_child', engine, if_exists='append', index=False)


for file in os.listdir(folder_path):
    if file.startswith('H_ECOM_ORDER'):
        df = pd.read_csv(os.path.join(folder_path, file))
        engine.execute("TRUNCATE TABLE H_ECOM_Orders")
        df.to_sql('H_ECOM_Orders', engine, if_exists='append', index=False)

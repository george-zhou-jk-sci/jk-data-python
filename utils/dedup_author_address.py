
import requests
from bs4 import BeautifulSoup
import pandas as pd
# import pyodbc
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
import json
from dateutil.parser import parse
import time
import random
from sqlalchemy import create_engine
import  re

with open(Path(__file__).parent / "../config/mysql_config_adhoc.json", 'r') as config_file:
    # parse file
    config = json.loads(config_file.read())
    user = config['user']
    password = config['password']
    host = config['host']
    database = config['database']
    sql_conn_df = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
# print(config)
with mysql.connector.connect(**config) as sql_conn:
# sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=Georgetz.com,12154; DATABASE=ACS;   UID=datarw;   PWD=Toronto2020')
# sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=George-PC; DATABASE=ACS;   UID=datarw;   PWD=Toronto2020')
    with sql_conn.cursor() as cursor:
        query = 'select distinct author_name from ACS.author_china_10_yr_dedup;'
#
        author_list = pd.read_sql(query, sql_conn_df)

        for i in range(0, len(author_list)):
            author_name = author_list.iloc[i, 0]
            cursor.execute(f"call mat_views.dedup_author_address('{author_name}', 0.4);")
            cursor.execute(f"""insert into adhoc_and_tests.dedup_author_address
                                select * from mat_views._dedup_author_address;""")
            sql_conn.commit()
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from pathlib import Path
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from datetime import datetime


def get_TCI_sku(url, link_id, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:


            browser.get(url)
            time.sleep(2)
            browser.refresh()

            time.sleep(5)
            href = ""
            sku = ""
            # res = requests.get(url)
            # print(res.headers)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            # print(soup.prettify())
            # print(soup.prettify())
            for lis in soup.find_all('li', class_="list-group-item"):
                # print(lis)
                for a_s in lis.find_all('div', class_="search-result-number"):
                    # print(a_s)
                    if a_s.find_all('span') != []:
                        sku = str(a_s.find_all('span')[0].text).strip().replace("'", "''")
                    for a in a_s.find_all('a', href=True):

                        if re.match(".*/catalog/.*", str(a['href'])):
                            if re.match(".*alfa.com.*", str(a['href'])):
                                href = str(a['href']).strip().replace("'", "''")
                            else:
                                href = f"""https://www.alfa.com{str(a['href']).strip().replace("'", "''")}"""
                            # for SKU_crude in SKUs_crude:
                            #     print(f"Found URL: {link}")
                        cursor.execute(f"insert into alfa_list ("
                                       f"sku, url) values ("
                                       f"'{sku}', '{href}')")
            cursor.commit()



    except Exception as e:
        with open(Path(__file__).parent / "../log/alfa_link_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



if __name__ == "__main__":
    try:
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        # fireFoxOptions.headless = False

        with webdriver.Firefox(
                executable_path=Path(__file__).parent / "../driver/geckodriver.exe",
                options=fireFoxOptions) as browser:
            with pyodbc.connect(
                    'DRIVER={ODBC Driver 17 for SQL Server}; '
                    'SERVER=jk-sci-com.cebe5ycuiwaa.us-east-2.rds.amazonaws.com; '
                    'DATABASE=Chem_Pricing;   UID=datarw;   PWD=Toronto2020') as sql_conn:
                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                # sku = pd.read_sql("select top(10) id, isnull(sku, 'N/A') as SKU from TCI_list "
                #                   "where assigned = 0 and retrieved = 0 order by newid()", sql_conn)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                sku_link.rename(columns={0:"sku_link"}, inplace=True)
                sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku_link":["https://www.alfa.com/zh-cn/inorganic-acids/"]})
                # print(sku_link)
                # sku_link = pd.read_sql("select id, isnull(sku_link, 'N/A') as sku_link from TCI_link", sql_conn)
                # if len(sku_link) != 0:
                #     update_sql = "Update [TCI_list] set Assigned=1 where [id] in (" + ', '.join(
                #         list(map(str, sku_link.iloc[:, 0].tolist()))) + ")"
                #
                #     sql_conn.execute(update_sql)
                #     sql_conn.commit()
                #
                for index, row in sku_link.iterrows():
                    try:
                        if row.sku_link != "N/A" and str(row.sku_link).strip() != "":
                            url = row.sku_link
                    # cursor = sql_conn.cursor()
                            print(f"{datetime.now()}, Processing {index + 1} url, total {len(sku_link)}, url:{url}")
                            get_TCI_sku(url, row.id, browser, sql_conn)
                            time.sleep(2)
                    except Exception as e:
                        with open(Path(__file__).parent / "../log/alfa_link_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        with open(Path(__file__).parent / "../log/alfa_link_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
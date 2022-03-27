import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from pathlib import Path
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import json
import random
from sqlalchemy import create_engine
import mysql.connector
import threading
from datetime import datetime


def get_Alfa_sku(url, link_id, sku, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:


            browser.get(url)
            # time.sleep(2)
            browser.refresh()
            for text_area in browser.find_elements(By.CLASS_NAME,"input-quantity"):
                text_area.send_keys("1000")
            time.sleep(5)
            href = ""
            sku = ""
            title = ""
            cas = ""
            synonym = ""
            stock_num = ""
            size = ""
            market_price = ""
            stock = ""
            # res = requests.get(url)
            # print(res.headers)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            # print(soup.prettify())
            # print(soup.prettify())
            for main_title in soup.find_all('div', class_="col-xs-12 col-sm-12 col-md-9 col-lg-9"):
                if main_title.find_all('h1') != []:
                    title = str(main_title.find_all('h1')[0].text).strip().replace("'", "''")
            for main_section in soup.find_all('div', class_="col-xs-12 col-sm-12 col-md-12 col-lg-12"):
                if main_section.find_all("div", class_="col-md-4") != []:
                    if len(main_section.find_all("div", class_="col-md-4")) > 0:
                        cas = str(main_section.find_all("div", class_="col-md-4")[0].text).strip().replace("'", "''")
                    if len(main_section.find_all("div", class_="col-md-4")) > 1:
                        synonym = str(main_section.find_all("div", class_="col-md-4")[1].text).strip().replace("'", "''")
            for main_stocks in soup.find_all("div", class_="pricebox-parent"):
                if main_stocks.find_all("tr", class_="item-row") != []:
                    for trs in main_stocks.find_all("tr", class_="item-row"):
                        if len(trs.find_all("td")) > 0:
                            stock_num = str(trs.find_all("td")[0].text).strip().replace("'", "''")
                        if len(trs.find_all("td")) > 1:
                            size = str(trs.find_all("td")[1].text).strip().replace("'", "''")
                        if len(trs.find_all("td")) > 2:
                            market_price = str(trs.find_all("td")[2].text).strip().replace("'", "''")
                        if len(trs.find_all("td")) > 4:
                            stock = str(trs.find_all("td")[4].text).strip().replace("'", "''")
                        cursor.execute(f"insert into alfa_sku ("
                                       f"cas, stock_num, size, market_price,"
                                       f" stock, title, synonym, link_id, url, sku) values ("
                                       f"'{cas}', '{stock_num}', '{size}', '{market_price}',"
                                       f"'{stock}', '{title}', '{synonym}', '{link_id}', '{url}', '{sku}')")

                        # print(title)
                        # print(cas)
                        # print(synonym)
                        # print(stock_num)
                        # print(size)
                        # print(market_price)
                        # print(stock)
                        # print("\n\n")
            # for lis in soup.find_all('li', class_="list-group-item"):
            #     # print(lis)
            #     for a_s in lis.find_all('div', class_="search-result-number"):
            #         # print(a_s)
            #         if a_s.find_all('span') != []:
            #             sku = str(a_s.find_all('span')[0].text).strip().replace("'", "''")
            #         for a in a_s.find_all('a', href=True):
            #
            #             if re.match(".*/catalog/.*", str(a['href'])):
            #                 if re.match(".*alfa.com.*", str(a['href'])):
            #                     href = str(a['href']).strip().replace("'", "''")
            #                 else:
            #                     href = f"""https://www.alfa.com{str(a['href']).strip().replace("'", "''")}"""
                            # for SKU_crude in SKUs_crude:
                            #     print(f"Found URL: {link}")


            #
            conn.commit()
            with conn.cursor() as cursor_update:
                update_sql = f"Update alfa_list set retrieved=1 where id = {link_id}"

                cursor_update.execute(update_sql)
                conn.commit()


    except Exception as e:
        with open(Path(__file__).parent / "../log/alfa_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



def run_alfa():
    try:
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver")
        # fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")

        with open(Path(__file__).parent / "../config/mysql_config_chem_pricing_stg.json", 'r') as config_file:
            # parse file
            config = json.loads(config_file.read())
            user = config['user']
            password = config['password']
            host = config['host']
            database = config['database']
            sql_conn_df = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
        # print(config)

        with webdriver.Firefox(
                # executable_path=Path(__file__).parent / "../driver/geckodriver.exe",
                service=fireFoxService,
                options=fireFoxOptions) as browser:
            with mysql.connector.connect(**config) as sql_conn:
                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                sku_link = pd.read_sql("select id, ifnull(sku, 'N/A') as sku, ifnull(url, 'N/A') as sku_link from alfa_list "
                                  "where assigned = 0 and retrieved = 0 order by rand() limit 10", sql_conn_df)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                # sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku_link":["https://www.alfa.com/zh-cn/catalog/056166/"]})
                print(sku_link)
                # sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                if len(sku_link) != 0:
                    with sql_conn.cursor() as cursor_assign:
                        update_sql = "Update alfa_list set assigned=1 where id in (" + ', '.join(
                            list(map(str, sku_link.iloc[:, 0].tolist()))) + ")"

                        cursor_assign.execute(update_sql)
                        sql_conn.commit()
                #
                for index, row in sku_link.iterrows():
                    try:
                        if row.sku_link != "N/A" and str(row.sku_link).strip() != "":
                            url = row.sku_link
                    # cursor = sql_conn.cursor()
                            print(f"{datetime.now()}, Processing {index + 1} url, total {len(sku_link)}, url:{url}")
                            get_Alfa_sku(url, row.id, row.sku, browser, sql_conn)
                            # time.sleep(2)
                    except Exception as e:
                        with open(Path(__file__).parent / "../log/alfa_SKU_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        with open(Path(__file__).parent / "../log/alfa_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")

if __name__ == "__main__":
    # run_alfa()

    t_1 = threading.Thread(target=run_alfa)
    t_2 = threading.Thread(target=run_alfa)
    t_3 = threading.Thread(target=run_alfa)
    t_4 = threading.Thread(target=run_alfa)

    t_1.start()
    t_2.start()
    t_3.start()
    t_4.start()

    t_1.join()
    t_2.join()
    t_3.join()
    t_4.join()


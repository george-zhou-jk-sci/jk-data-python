# import pyodbc
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

from sqlalchemy import create_engine

import threading
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import json
import random

def get_Alfa_sku(url, link_id, sku, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:


            browser.get(url)

            time.sleep(random.randint(5,10))
            soup = BeautifulSoup(browser.page_source, "html.parser")

            i = 0
            name = []
            cas = []

            href = url
            prod_name = ""
            prod_cas = ""
            brand = ""
            sku = ""
            size = ""
            stock = ""
            description = ""
            purity = ""
            market_price = ""
            off_price = ""

            for item_info in soup.find_all('div', class_="exh_text f_14 clr_6 fl"):
                if item_info.find_all('p', class_="f_20 clr_ora f_wei") != []:
                    name.append(str(item_info.find_all('p', class_="f_20 clr_ora f_wei")[0].text).replace("'", "''"))
                if item_info.find_all('a', class_="f_12 clr_6") != []:
                    cas.append(str(item_info.find_all('a', class_="f_12 clr_6")[0].text).replace("'", "''"))
                # print(name)
                # print(cas)
            for item_tables in soup.find_all('table', id="table_list"):

                i += 1
                if i <= len(name):
                    prod_name = name[i - 1]
                if i <= len(cas):
                    prod_cas = cas[i - 1]
                # print(prod_name)
                # print(prod_cas)
                for item_trs in item_tables.find_all('tr'):
                    if item_trs.find_all('td', class_="brand") != []:
                        brand = str(item_trs.find_all('td', class_="brand")[0].text).replace("'", "''")
                    if len(item_trs.find_all('td')) > 1:
                        sku = str(item_trs.find_all('td')[1].text).replace("'", "''")
                    if item_trs.find_all('td', class_="guige") != []:
                        size = str(item_trs.find_all('td', class_="guige")[0].text).replace("'", "''")
                    if item_trs.find_all('span', class_="stock") != []:
                        stock = str(item_trs.find_all('span', class_="stock")[0].text).replace("'", "''")
                    if item_trs.find_all('td', class_="desc") != []:
                        description = str(item_trs.find_all('td', class_="desc")[0].text).replace("'", "''")
                    if item_trs.find_all('td', class_="chundu") != []:
                        purity = str(item_trs.find_all('td', class_="chundu")[0].text).replace("'", "''")
                    if item_trs.find_all('td', class_="djrmb") != []:
                        market_price = str(item_trs.find_all('td', class_="djrmb")[0].text).replace("'", "''")
                    if item_trs.find_all('td', class_="offprice") != []:
                        off_price = str(item_trs.find_all('td', class_="offprice")[0].text).replace("'", "''")



                    if purity+sku+size+market_price+stock+brand+off_price+description+prod_name+prod_cas != "":

                        try:
                            cursor.execute(f"insert into innochem_all_cat_sku ("
                                           f"cas, size, market_price,"
                                           f" stock, purity, list_id, url, sku, "
                                           f"promo_price, description, brand,"
                                           f"name) values ("
                                           f"'{prod_cas}', '{size}', '{market_price}', "
                                           f"'{stock}', '{purity}', "
                                           f"'{link_id}', '{url}', '{sku}',  "
                                           f"'{off_price}', '{description}', '{brand}', "
                                           f"'{prod_name}');")

                            conn.commit()
                        except:
                            conn.rollback()
                            raise
                            # else:
                                # cursor.execute(f"insert into daicelchiraltech_sku ("
                                #        f"cas, size, market_price, stock, name_cn, name_en, list_id, url, sku, "
                                #        f"purity, sku_size, synonym_cn, synonym_en, formula, molecular_mass) values ("
                                #        f"'{cas}', '{size}', '{market_price}', '{stock}', '{name_cn}', '{name_en}', "
                                #        f"'{link_id}', '{url}', '{sku}', '{purity}', '{sku_size}', '{synonym_cn}', "
                                #        f"'{synonym_en}', '{formula}', '{mm}')")


                        # print(href)
                        # print(purity)
                        # # print(sku_size)
                        # print(sku)
                        # # print(name_cn)
                        # # print(name_en)
                        # # print(cas)
                        # # print(synonym_cn)
                        # # print(synonym_en)
                        # # print(stock_num)
                        # print(size)
                        # print(market_price)
                        # # print(promo_price)
                        # # print(vip_price)
                        # print(stock)
                        # print(brand)
                        # # print(formula)
                        # # print(mm)
                        # # print(category)
                        # # print(size_unit)
                        # print(brand)
                        # # print(shipping_speed)
                        # # print(region)
                        # print(off_price)
                        # print(description)
                        # print(prod_name)
                        # print(prod_cas)
                        # print("\n\n")
#            # for lis in soup.find_all('li', class_="list-group-item"):
#            #     # print(lis)
#            #     for a_s in lis.find_all('div', class_="search-result-number"):
#            #         # print(a_s)
#            #         if a_s.find_all('span') != []:
#            #             sku = str(a_s.find_all('span')[0].text).strip().replace("'", "''")
#            #         for a in a_s.find_all('a', href=True):
#            #
#            #             if re.match(".*/catalog/.*", str(a['href'])):
#            #                 if re.match(".*alfa.com.*", str(a['href'])):
#            #                     href = str(a['href']).strip().replace("'", "''")
#            #                 else:
#            #                     href = f"""https://www.alfa.com{str(a['href']).strip().replace("'", "''")}"""
                            # for SKU_crude in SKUs_crude:
                            #     print(f"Found URL: {link}")


            #
##            # cursor.commit()
        with conn.cursor() as cursor_update:
            update_sql = f"Update innochem_all_cat_link set retrieved=1 where ID = {link_id};"

            cursor_update.execute(update_sql)
            conn.commit()




    except Exception as e:
        # raise
        with open(Path(__file__).parent / "../log/innochem_all_cat_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



def run_alfa():
    try:
        time.sleep(random.randint(5, 10))
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        # fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver")
        fireFoxService=Service(Path(__file__).parent / "../driver/geckodriver.exe")
        # fireFoxOptions.headless = False
        with open(Path(__file__).parent / "../config/mysql_config.json", 'r') as config_file:

            # parse file
            config = json.loads(config_file.read())
            user = config['user']
            password = config['password']
            host = config['host']
            database = config['database']
        sql_conn_df = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

        with webdriver.Firefox(
                # executable_path=Path(__file__).parent / "../driver/geckodriver",
                #executable_path=Path(__file__).parent / "../driver/geckodriver.exe",
                service=fireFoxService,
                options=fireFoxOptions) as browser:

            with mysql.connector.connect(**config) as sql_conn:
                # cursor = sql_conn.cursor()
                # query = f"select count(*) count from Alfa_List"
                # cursor.execute(query)
                # for count in cursor:
                #     print(f"the count is {count}")
                # cursor.close()

                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                sku_link = pd.read_sql("select id as id, ifnull(sku, 'N/A') as sku, url as sku_link from innochem_all_cat_link "
                                       "where assigned=0 and retrieved=0 order by RAND() limit 3000;", sql_conn_df)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                # sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku_link":["https://www.alfa.com/zh-cn/catalog/056166/"]})
                # print(sku_link)
                ### sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                if len(sku_link) != 0:
                   with sql_conn.cursor() as cursor:
                       update_sql = "Update innochem_all_cat_link set assigned=1 where id in (" + ', '.join(
                           list(map(str, sku_link.iloc[:, 0].tolist()))) + ");"

                       cursor.execute(update_sql)
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
                        # raise
                        with open(Path(__file__).parent / "../log/innochem_all_cat_SKU_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        # raise
        with open(Path(__file__).parent / "../log/innochem_all_cat_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
#
if __name__ == "__main__":
    # run_alfa()
    #
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
#

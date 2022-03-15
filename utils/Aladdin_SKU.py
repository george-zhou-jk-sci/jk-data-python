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
import mysql.connector
from mysql.connector import errorcode
import threading
from datetime import datetime
import json
import random
from sqlalchemy import create_engine


def get_Alfa_sku(url, link_id, sku, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:


            browser.get(url)
            time.sleep(int(random.random() * 10))
            # time.sleep(2)
            # browser.refresh()
            # for text_area in browser.find_elements(By.CLASS_NAME,"input-quantity"):
            #     text_area.send_keys("1000")
            # check "div", "spinner spinnerClock", wait if exist, or continue
            # while len(browser.find_elements(By.CLASS_NAME, "spinner spinnerClock")) > 0:
            #     time.sleep(1)
            # time.sleep(5)
            is_execute = False
            href = ""
            sku = ""
            brand = ""
            title_cn = ""
            title_en = ""
            cas = ""
            synonym = ""
            stock_shanghai = []
            stock_tianjing = []
            item_list = []
            stock_num = ""
            size = ""
            market_price = ""
            stock = ""
            title_cn = ""
            title_en = ""
            purity = ""
            bulk_ship = ""
            cas = ""
            size = []
            market_price = []
            # stock_shanghai = ""
            # stock_shenzheng = ""
            # stock_tianjing = ""
            # stock_wuhan = ""
            # stock_chengdu = ""
            discount_price = []
            # vip_price = ""
            # res = requests.get(url)
            # print(res.headers)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            # print(soup.prettify())
            # print(soup.prettify())
            for main_title in soup.find_all('div', class_="product-info-main"):
                if main_title.find_all('h1', class_="page-title") != []:
                    title_cn = str(main_title.find_all('h1', class_="page-title")[0].text).strip().replace("'", "''")
                for skus in main_title.find_all("div", class_="product attribute sku"):
                    if skus.find_all("div", class_="value") != []:
                        sku = str(skus.find_all("div", class_="value")[0].text).strip().replace("'", "''")

                if main_title.find_all('div', class_="product-package") != []:
                    purity = str(main_title.find_all('div', class_="product-package")[0].text).strip().replace("'", "''")
                for main_shop in main_title.find_all('div', class_="product-shop"):
                    for main_infos in main_shop.find_all("div", class_="product-info"):
                        if main_infos.find_all("a") != []:
                            cas = str(main_infos.find_all("a")[0].text).strip().replace("'", "''")
                        if main_infos.find_all("span", class_="brand-style") != []:
                            brand = str(main_infos.find_all("span", class_="brand-style")[0].text).strip().replace("'", "''")

                    if main_shop.find_all('div', class_="product-name2-regent") != []:
                        title_en = str(main_shop.find_all('div', class_="product-name2-regent")[0].text).strip().replace("'", "''")
                for main_section in soup.find_all('div', class_="table-wrapper grouped"):
                    # stocking
                    if main_section.find_all("div", class_="mask hide_stock_page") != []:
                        count_stock = len(main_section.find_all('div', class_="mask hide_stock_page"))
                        for stock_infos in main_section.find_all('div', class_="mask hide_stock_page"):
                            for prompt_infos in stock_infos.find_all('div', class_="prompt_text"):
                                if prompt_infos.find_all('p', id=re.compile("^sh_stock_show")) != []:
                                    stock_shanghai.append(str(prompt_infos.find_all('p', id=re.compile("^sh_stock_show"))[0].text).strip().replace("'", "''"))
                                else:
                                    stock_shanghai.append("")
                                if prompt_infos.find_all('p', id=re.compile("^tj_stock_show")) != []:
                                    stock_tianjing.append(str(prompt_infos.find_all('p', id=re.compile("^tj_stock_show"))[0].text).strip().replace("'", "''"))
                                else:
                                    stock_tianjing.append("")

                    # pricing
                    for main_tables in main_section.find_all('table', id="super-product-table"):
                        for tbody in main_tables.find_all('tbody'):
                            if tbody.find_all('tr') != []:
                                count_price = len(tbody.find_all('tr'))
                                for trs in tbody.find_all('tr'):
                                    if len(trs.find_all('td')) > 0:
                                        sku_size = str(trs.find_all('td')[0].text).strip().replace("'", "''")
                                        sku_size_list = sku_size.split('-')
                                        if len(sku_size_list) > 0:
                                            size.append(str(sku_size_list[-1]).strip().replace("'", "''"))
                                    if len(trs.find_all('td', class_="ajaxPrice")) > 0:
                                        for prices in trs.find_all('td', class_="ajaxPrice"):
                                            if len(prices.find_all('span', class_="o-price price")) > 0:
                                                market_price.append(
                                                    str(prices.find_all('span', class_="o-price price")[0].text)
                                                        .strip().replace("'", "''"))
                                            else:
                                                market_price.append("")
                                            if len(prices.find_all('span', class_="s-price price")) > 0:
                                                discount_price.append(
                                                    str(prices.find_all('span', class_="s-price price")[0].text)
                                                        .strip().replace("'", "''"))
                                            else:
                                                discount_price.append("")
                                    else:
                                        market_price.append("")
                                        discount_price.append("")
                    # check if all correct
                    if count_price == count_stock:
                        item_info_dict = {"stock_shanghai":stock_shanghai, "stock_tianjing":stock_tianjing, "size":size,
                                          "market_price":market_price, "discount_price":discount_price}
                        item_info = pd.DataFrame(item_info_dict)
                        for index, row in item_info.iterrows():
                            # print(row["size"])
                            # print(type(row.size))
                            # print(row)
                            cursor.execute(f"insert into aladdin_sku ("
                                           f"title_cn, title_en, purity,"
                                           f"cas, size, market_price, brand,"
                                           f"stock_shanghai, stock_tianjing,"
                                           f"discount_price,"
                                           f"link_id, url, sku) values ("
                                           f"'{title_cn}', '{title_en}', '{purity}',"
                                           f"'{cas}', '{str(row['size'])}', '{row.market_price}', '{brand}',"
                                           f"'{row.stock_shanghai}', '{row.stock_tianjing}',"
                                           f"'{row.discount_price}',"
                                           f"'{link_id}', '{url}', '{sku}')")
                        conn.commit()
                        is_execute = True

                            # print(title_cn)
                            # print(title_en)
                            # print(purity)
                            # print(brand)
                            # print(cas)
                            # # print(row, "\n\n\n")
                            # print(row.size)
                            # print(row.market_price)
                            #
                            # print(row.stock_shanghai)
                            #
                            # print(row.stock_tianjing)
                            #
                            # print(row.discount_price)

                            # print("\n\n")



            #
            if is_execute:
                with conn.cursor() as cursor_retrieve:
                    update_sql = f"Update aladdin_list set retrieved=1 where id = {link_id}"

                    cursor_retrieve.execute(update_sql)
                    conn.commit()


    except Exception as e:
        with open(Path(__file__).parent / "../log/Aladdin_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



def run_alfa():
    try:
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        # fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver")
        fireFoxService=Service(Path(__file__).parent / "../driver/geckodriver.exe")
        # fireFoxOptions.headless = False
        with open(Path(__file__).parent / "../config/mysql_config_chem_pricing_stg.json", 'r') as config_file:
            # parse file
            config = json.loads(config_file.read())
            user = config['user']
            password = config['password']
            host = config['host']
            database = config['database']
            sql_conn_df = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

        with webdriver.Firefox(
                    # executable_path=Path(__file__).parent / "../driver/geckodriver.exe",
                    service=fireFoxService,
                    options=fireFoxOptions) as browser:
                with mysql.connector.connect(**config) as sql_conn:
                    # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                    sku_link = pd.read_sql("select id, '' as sku, ifnull(link_item, 'N/A') as sku_link from aladdin_list "
                                      "where assigned = 0 and retrieved = 0 order by rand() limit 1", sql_conn_df)
                    # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                    # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                    # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                    # sku_link["id"] = range(1, len(sku_link) + 1)

                    # sku_link = pd.DataFrame({"id":[1], "sku":"", "sku_link":["https://www.aladdin-e.com/zh_cn/d154604.html"]})
                    print(sku_link)
                    # sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                    if len(sku_link) != 0:
                        with sql_conn.cursor() as cursor_assign:
                            update_sql = "Update aladdin_list set assigned=1 where id in (" + ', '.join(
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
                            with open(Path(__file__).parent / "../log/Aladdin_SKU_error.csv", "a") as f:
                                f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                                f.write("\n")
                            pass
    except Exception as e:
        with open(Path(__file__).parent / "../log/Aladdin_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")

if __name__ == "__main__":
    run_alfa()

    # t_1 = threading.Thread(target=run_alfa)
    # t_2 = threading.Thread(target=run_alfa)
    # t_3 = threading.Thread(target=run_alfa)
    # t_4 = threading.Thread(target=run_alfa)
    #
    # t_1.start()
    # t_2.start()
    # t_3.start()
    # t_4.start()
    #
    # t_1.join()
    # t_2.join()
    # t_3.join()
    # t_4.join()
#

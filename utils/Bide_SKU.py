#import pyodbc
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
import threading
from datetime import datetime
import json
import mysql.connector
from mysql.connector import errorcode


def get_Alfa_sku(url, link_id, sku, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:


            browser.get(url)
            # time.sleep(2)
            # browser.refresh()
            # for text_area in browser.find_elements(By.CLASS_NAME,"input-quantity"):
            #     text_area.send_keys("1000")
            # check "div", "spinner spinnerClock", wait if exist, or continue
            # while not len(browser.find_elements(By.CLASS_NAME, "products-top-table")) > 0:
            #     time.sleep(1)
            while len(browser.find_elements(By.CLASS_NAME, "spinner spinnerClock")) > 0:
                time.sleep(1)
            time.sleep(5)
            is_execute = False
            href = ""
            # sku = ""
            title_cn = ""
            title_en = ""
            cas = ""
            synonym = ""
            stock_num = ""
            size = ""
            market_price = ""
            stock = ""
            title_cn = ""
            title_en = ""
            purity = ""
            bulk_ship = ""
            cas = ""
            size = ""
            market_price = ""
            stock_shanghai = ""
            stock_shenzheng = ""
            stock_tianjing = ""
            stock_wuhan = ""
            stock_chengdu = ""
            discount_price = ""
            vip_price = ""
            # res = requests.get(url)
            # print(res.headers)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            # print(soup.prettify())
            # print(soup.prettify())
            for main_title in soup.find_all('h2', class_="products-name"):
                if main_title.find_all('span', class_="sp_pro_name_cn") != []:
                    title_cn = str(main_title.find_all('span', class_="sp_pro_name_cn")[0].text).strip().replace("'",
                                                                                                                 "''")
                if main_title.find_all('span', class_="sp_pro_name_en") != []:
                    title_en = str(main_title.find_all('span', class_="sp_pro_name_en")[0].text).strip().replace("'",
                                                                                                                 "''")
            for main_skus in soup.find_all('div', class_="products-top-kuang"):
                if main_skus.find_all('b', id="first_bd") != []:
                    sku = str(main_skus.find_all('b', id="first_bd")[0].text).strip().replace("'", "''")
            for main_section in soup.find_all('div', class_="tab-content-item tab-content-show"):
                if main_section.find_all("tr") != []:
                    for trs in main_section.find_all("tr"):
                        if len(trs.find_all("td")) > 1:
                            if re.match(".*CAS.*", str(trs.find_all("td")[0].text).strip().replace("'", "''")):
                                cas = str(trs.find_all("td")[1].text).strip().replace("'", "''")
            #         if len(main_section.find_all("div", class_="col-md-4")) > 1:
            # #             synonym = str(main_section.find_all("div", class_="col-md-4")[1].text).strip().replace("'", "''")
            for main_stocks in soup.find_all("div", class_="products-top-table products-table"):
                for tbody in main_stocks.find_all("tbody"):
                    if tbody.find_all("tr") != []:
                        for trs in tbody.find_all("tr"):
                            if len(trs.find_all("td")) > 0:
                                purity = str(trs.find_all("td")[0].text).strip().replace("'", "''")
                            if len(trs.find_all("td")) > 1:
                                size = str(trs.find_all("td")[1].text).strip().replace("'", "''")
                            if len(trs.find_all("td")) > 2:
                                if len(trs.find_all("td")) < 4:
                                    bulk_ship = str(trs.find_all("td")[2].text).strip().replace("'", "''")
                                else:
                                    market_price = str(trs.find_all("td")[2].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 3:
                                        stock_shanghai = str(trs.find_all("td")[3].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 4:
                                        stock_shenzheng = str(trs.find_all("td")[4].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 5:
                                        stock_tianjing = str(trs.find_all("td")[5].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 6:
                                        stock_wuhan = str(trs.find_all("td")[6].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 7:
                                        stock_chengdu = str(trs.find_all("td")[7].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 8:
                                        discount_price = str(trs.find_all("td")[8].text).strip().replace("'", "''")
                                    if len(trs.find_all("td")) > 9:
                                        vip_price = str(trs.find_all("td")[9].text).strip().replace("'", "''")

                            cursor.execute(f"insert into bide_sku ("
                                           f"title_cn, title_en, purity, bulk_ship,"
                                           f"cas, size, market_price, "
                                           f"stock_shanghai, stock_shenzheng, stock_tianjing,"
                                           f"stock_wuhan, stock_chengdu, discount_price, vip_price,"
                                           f"link_id, url, sku) values ("
                                           f"'{title_cn}', '{title_en}', '{purity}', '{bulk_ship}',"
                                           f"'{cas}', '{size}', '{market_price}',"
                                           f"'{stock_shanghai}', '{stock_shenzheng}', '{stock_tianjing}',"
                                           f"'{stock_wuhan}', '{stock_chengdu}', '{discount_price}',"
                                           f"'{vip_price}', '{link_id}', '{url}', '{sku}')")
                            is_execute = True
                            conn.commit()
                            # print(f"insert into Bide_sku ("
                            #                f"title_cn, title_en, purity, bulk_ship,"
                            #                f"cas, size, market_price, "
                            #                f"stock_shanghai, stock_shenzheng, stock_tianjing,"
                            #                f"stock_wuhan, stock_chengdu, discount_price, vip_price,"
                            #                f"link_id, url, sku) values ("
                            #                f"'{title_cn}', '{title_en}', '{purity}', '{bulk_ship}',"
                            #                f"'{cas}', '{size}', '{market_price}',"
                            #                f"'{stock_shanghai}', '{stock_shenzheng}', '{stock_tianjing}',"
                            #                f"'{stock_wuhan}', '{stock_chengdu}', '{discount_price}',"
                            #                f"'{vip_price}', '{link_id}', '{url}', '{sku}')")
                                    # print(title_cn)
                                    # print(title_en)
                                    # print(purity)
                                    # print(bulk_ship)
                                    # print(cas)
                                    # print(size)
                                    # print(market_price)
                                    # print(stock_shanghai)
                                    # print(stock_shenzheng)
                                    # print(stock_tianjing)
                                    # print(stock_wuhan)
                                    # print(stock_chengdu)
                                    # print(discount_price)
                                    # print(vip_price)
                                    # print("\n\n")



            #
            if is_execute:
                # print(is_execute)
                update_sql = f"Update bide_list set Retrieved=1 where id = {link_id}"

                cursor.execute(update_sql)
                conn.commit()


    except Exception as e:
        with open(Path(__file__).parent / "../log/Bide_SKU_error.csv", "a") as f:
            print(e)
            f.write(str(e))
            f.write("\n")
        pass
        # raise



def run_alfa():
    try:
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        # fireFoxOptions.headless = False
        with open(Path(__file__).parent / "../config/mysql_config.json", 'r') as config_file:

            # parse file
            config = json.loads(config_file.read())
        with webdriver.Firefox(
                executable_path=Path(__file__).parent / "../driver/geckodriver",
                options=fireFoxOptions) as browser:
            with mysql.connector.connect(**config) as sql_conn:
                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                sku_link = pd.read_sql("select id, ifnull(sku, 'N/A') as sku, ifnull(Link_Item, 'N/A') as sku_link from bide_list "
                                  "where assigned = 0 and retrieved = 0 order by RAND() limit 3000", sql_conn)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                # sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku":"1320-06-5", "sku_link":["https://www.bidepharm.com/products/1320-06-5.html"]})
                print(sku_link)
                # sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                if len(sku_link) != 0:
                    update_sql = "Update bide_list set Assigned=1 where id in (" + ', '.join(
                        list(map(str, sku_link.iloc[:, 0].tolist()))) + ")"
                    with sql_conn.cursor() as cursor:
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
                        with open(Path(__file__).parent / "../log/Bide_SKU_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        with open(Path(__file__).parent / "../log/Bide_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        # raise

if __name__ == "__main__":
    run_alfa()
    #
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

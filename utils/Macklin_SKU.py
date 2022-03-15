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
import threading
from datetime import datetime
import random
import json
import mysql.connector
from mysql.connector import errorcode
import random
from sqlalchemy import create_engine

# def refresh(driver):
#     try:
#         element = WebDriverWait(driver, 10)#.until(
#         #    EC.presence_of_element_located((By.CLASS_NAME, "button"))
#         #)
#     except:
#         driver.refresh()
#         refresh()

def get_Alfa_sku(url, link_id, sku, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:

            browser.get(url)

            # for i in range(0, 10):
            #     i += 1
            #     try:
            #         browser.get(url)
            #         time.sleep(random.randint(5, 10))
            #         break
            #     except:
            #         time.sleep(random.randint(5, 10))
            #         browser.refresh()


            time.sleep(random.randint(5,10))
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
            # stock_shanghai = []
            # stock_tianjing = []
            # item_list = []
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
            # stock_shanghai = ""
            # stock_shenzheng = ""
            # stock_tianjing = ""
            # stock_wuhan = ""
            # stock_chengdu = ""
            discount_price = ""
            # vip_price = ""
            # res = requests.get(url)
            # print(res.headers)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            # print(soup.prettify())
            # print(soup.prettify())
            for main_title in soup.find_all('div', class_="product-detail"):
                if main_title.find_all('h1') != []:
                    title_cn = str(main_title.find_all('h1')[0].text).strip().replace("'", "''")
                for product_infos in main_title.find_all('div', class_="product-general"):
                    if product_infos.find_all('span')[0] != []:
                        title_en = str(product_infos.find_all('span')[0].text).strip().replace("'", "''")

                    for info_tables in product_infos.find_all("table", class_="productOverViewInfo"):
                        if len(info_tables.find_all('tr')) > 0:
                            if info_tables.find_all('tr')[0].find_all('td') != []:
                                synonym = str(info_tables.find_all('tr')[0].find_all('td')[0].text).strip().replace("'", "''")
                        if len(info_tables.find_all('tr')) > 1:
                            if info_tables.find_all('tr')[1].find_all('td') != []:
                                cas = str(info_tables.find_all('tr')[1].find_all('td')[0].text).strip().replace("'", "''")
                        # if info_ta.find_all("div", class_="value") != []:
                        #     sku = str(skus.find_all("div", class_="value")[0].text).strip().replace("'", "''")


                for main_shop in main_title.find_all('div', class_="shopping"):
                    for main_infos in main_shop.find_all("table", class_="table table-bordered"):
                        for tbody in main_infos.find_all('tbody'):
                            for trs in tbody.find_all("tr"):
                                if len(trs.find_all('td')) > 0:
                                    sku_size = str(trs.find_all('td')[0].text).strip().replace("'", "''")
                                    sku_size_list = sku_size.split('-')
                                    if len(sku_size_list) > 0:
                                        sku = str(sku_size_list[0]).strip().replace("'", "''")
                                        size = str(sku_size_list[-1]).strip().replace("'", "''")
                                if len(trs.find_all('td')) > 1:
                                    purity = str(trs.find_all('td')[1].text).strip().replace("'", "''")
                                if len(trs.find_all('td')) > 2:
                                    stock = str(trs.find_all('td')[2].text).strip().replace("'", "''")
                                if len(trs.find_all('td')) > 3:
                                    market_price = str(trs.find_all('td')[3].text).strip().replace("'", "''")
                                if len(trs.find_all('td')) > 4:
                                    discount_price = str(trs.find_all('td')[4].text).strip().replace("'", "''")

                                # print(row["size"])
                                # print(type(row.size))
                                # print(row)
                                cursor.execute(f"insert into macklin_sku ("
                                               f"title_cn, title_en, purity,"
                                               f"cas, size, market_price,"
                                               f"stock, synonym,"
                                               f"discount_price,"
                                               f"link_id, url, sku) values ("
                                               f"'{title_cn}', '{title_en}', '{purity}',"
                                               f"'{cas}', '{size}', '{market_price}',"
                                               f"'{stock}', '{synonym}',"
                                               f"'{discount_price}',"
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
                update_sql = f"Update macklin_list set Retrieved=1 where id = {link_id}"
                with conn.cursor() as cursor:
                    cursor.execute(update_sql)
                    conn.commit()


    except Exception as e:
        with open(Path(__file__).parent / "../log/Macklin_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



def run_alfa():
    try:
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        # fireFoxOptions.headless = False
        # fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver")
        fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")

        profile = webdriver.FirefoxProfile()
        # 1 - Allow all images
        # 2 - Block all images
        # 3 - Block 3rd party images
        profile.set_preference("permissions.default.image", 2)

        with open(Path(__file__).parent / "../config/mysql_config_chem_pricing_stg.json", 'r') as config_file:

            # parse file
            config = json.loads(config_file.read())
            user = config['user']
            password = config['password']
            host = config['host']
            database = config['database']
            sql_conn_df = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

        with webdriver.Firefox(
                # executable_path=Path(__file__).parent / "../driver/geckodriver",
                service=fireFoxService,
                options=fireFoxOptions,
                firefox_profile=profile) as browser:
            browser.set_page_load_timeout(30)
            browser.implicitly_wait(10)
            with mysql.connector.connect(**config) as sql_conn:
                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                sku_link = pd.read_sql("select id, ifnull(sku, 'N/A') as sku, ifnull(link_sku, 'N/A') as sku_link from  macklin_list "
                                  "where assigned = 0 and retrieved = 0 order by RAND() limit 10", sql_conn_df)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                # sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku":"T818796", "sku_link":["http://www.macklin.cn/products/T818796"]})
                # print(sku_link)
                # sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                if len(sku_link) != 0:
                    update_sql = "Update macklin_list set Assigned=1 where id in (" + ', '.join(
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
                            # print(f"{datetime.now()}, Processing {index + 1} url, total {len(sku_link)}, url:{url}")
                            get_Alfa_sku(url, row.id, row.sku, browser, sql_conn)
                            # time.sleep(2)
                    except Exception as e:
                        with open(Path(__file__).parent / "../log/Macklin_SKU_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        with open(Path(__file__).parent / "../log/Macklin_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")

if __name__ == "__main__":
    run_alfa()
    # for i in range(1,101):
    #     print(f"processing the {i}th round")
    #    t_1 = threading.Thread(target=run_alfa)
    #    t_2 = threading.Thread(target=run_alfa)
    #    t_3 = threading.Thread(target=run_alfa)
    #    t_4 = threading.Thread(target=run_alfa)
    #    t_5 = threading.Thread(target=run_alfa)
    #    t_6 = threading.Thread(target=run_alfa)
    #    t_7 = threading.Thread(target=run_alfa)
    #    t_8 = threading.Thread(target=run_alfa)
    #
    #    t_1.start()
    #    t_2.start()
    #    t_3.start()
    #    t_4.start()
    #    t_5.start()
    #    t_6.start()
    #    t_7.start()
    #    t_8.start()
    #
    #    t_1.join()
    #    t_2.join()
    #    t_3.join()
    #    t_4.join()
    #    t_5.join()
    #    t_6.join()
    #    t_7.join()
    #    t_8.join()
#
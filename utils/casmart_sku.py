# import pyodbc
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
            size_url = {}
            soup_size = BeautifulSoup(browser.page_source, "html.parser")
            for sizes_list in soup_size.find_all('i', {'id': 'recommendPro'}):
                for sizes in sizes_list.find_all('span'):
                    if sizes.find_all('a', href=True) != []:
                        size_url[str(sizes.find_all('a', href=True)[0].text).strip()
                            .replace("'", "''")] = f"""https://www.casmart.com.cn{str(
                            sizes.find_all('a', href=True)[0]['href']).strip().replace("'", "''")}"""

            # print(size_url)
            for key in size_url:


                # browser.refresh()
                # for text_area in browser.find_elements(By.CLASS_NAME,"input-quantity"):
                #     text_area.send_keys("1000")
                # time.sleep(5)
                title_text = ""
                main_td = []
                href = ""
                sku = ""
                name_cn = ""
                name_en = ""
                cas = ""
                synonym_cn = ""
                synonym_en = ""
                stock_num = ""
                size = ""
                market_price = ""
                promo_price = ""
                vip_price = ""
                stock = ""
                sku_size = ""
                brand = ""
                purity = ""
                formula = ""
                mm = ""
                category = ""
                size_unit = ""
                region = ""
                shipping_speed = ""
                pre_tax_price = ""
                freight = ""
                # res = requests.get(url)
                # print(res.headers)
                main_text_list = []
                main_text_dict = {}

                url = size_url[key]
                # print(f'key: {key}, url: {url}')
                browser.get(url)
                time.sleep(random.randint(5,10))
                soup = BeautifulSoup(browser.page_source, "html.parser")
                size = key

            # if soup.find_all('title') != []:
            #     title_text = str(soup.find_all('title')[0].text).strip()
            #
            # if title_text == '404 - 找不到文件或目录。':
            #     # No product
            #     print(f'link_id: {link_id}, sku: {sku}, url: {url} does not exist. Marking as 404.')
            #     with conn.cursor() as cursor_update:
            #         update_sql = f"Update casmart_list set Flag_404=1 where ID = {link_id};"
            #
            #         cursor_update.execute(update_sql)
            #         conn.commit()
                # pass
            # print(soup.prettify())
            # print(soup.prettify())
            # else:
                # pass
                for main_all in soup.find_all('div', {"class":"cas_blk14"}):
                    for main_info in main_all.find_all('div', {'class':'cas_b14_right'}):
                        if main_info.find_all('h2') != []:
                            name_cn = str(main_info.find_all('h2')[0].text).strip().replace("'", "''")
                        for price_box in main_info.find_all('div',{'id':'proPrice'}):
                            if price_box.find_all('h3') != []:
                                market_price = str(price_box.find_all('h3')[0].text).strip().replace("'", "''")

                        for main_text in main_info.find_all('div', {"class":"cas_b14_info01"}):
                            for texts in main_text.find_all('p'):
                                main_text_list.append(str(texts.text).strip().replace("'", "''"))
                            # print(main_text_list)

                            # Convert String List to Key-Value List dictionary
                            # Using split() + dictionary comprehension
                            main_text_dict = {sub[0]: '：'.join(sub[1:]) for sub in (ele.split('：') for ele in ([item for sublist in (elm.split('\xa0') for elm in main_text_list) for item in sublist]))}

                            for main_text_key in main_text_dict:
                                if main_text_key == '货号':
                                    sku = main_text_dict[main_text_key]
                                if main_text_key == 'CAS号':
                                    cas = main_text_dict[main_text_key]
                                # if main_text_key == '规格':
                                #     size = main_text_dict[main_text_key]
                                if main_text_key == '计量单位':
                                    size_unit = main_text_dict[main_text_key]
                                if main_text_key == '品牌':
                                    brand = main_text_dict[main_text_key]
                                if main_text_key == '区域':
                                    region = main_text_dict[main_text_key]
                                if main_text_key == '交货周期':
                                    shipping_speed = main_text_dict[main_text_key]
                                # if main_text_key == '':
                                #     sku = main_text_dict[main_text_key]
                                # if main_text_key == '':
                                #     sku = main_text_dict[main_text_key]
                            if main_text.find_all('p', {'id':'proAmount'}) != []:
                                stock = str(main_text.find_all('p', {'id':'proAmount'})[0].text).strip().replace("'", "''")
                            if main_text.find_all('p', {'id': 'proFreeDutyPri'}) != []:
                                pre_tax_price = str(main_text.find_all('p', {'id': 'proFreeDutyPri'})[0].text).strip().replace("'", "''")
                            if main_text.find_all('div', {'id': 'freight'}) != []:
                                freight = str(main_text.find_all('div', {'id': 'freight'})[0].text).strip().replace("'", "''")






                try:
                    cursor.execute(f"insert into casmart_sku ("
                                   f"cas, size, market_price,"
                                   f" stock, name_cn, list_id, url, sku, "
                                   f"shipping_speed, region, size_unit, brand,"
                                   f"pre_tax_price, freight) values ("
                                   f"'{cas}', '{size}', '{market_price}', "
                                   f"'{stock}', '{name_cn}', "
                                   f"'{link_id}', '{url}', '{sku}',  "
                                   f" '{shipping_speed}', '{region}', '{size_unit}', '{brand}', "
                                   f"'{pre_tax_price}', '{freight}');")
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
                # print(sku_size)
                # print(sku)
                # print(name_cn)
                # print(name_en)
                # print(cas)
                # print(synonym_cn)
                # print(synonym_en)
                # print(stock_num)
                # print(size)
                # print(market_price)
                # print(promo_price)
                # print(vip_price)
                # print(stock)
                # print(brand)
                # print(formula)
                # print(mm)
                # print(category)
                # print(size_unit)
                # print(brand)
                # print(shipping_speed)
                # print(region)
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
                update_sql = f"Update casmart_list set retrieved=1 where ID = {link_id};"

                cursor_update.execute(update_sql)
                conn.commit()




    except Exception as e:
        # raise
        with open(Path(__file__).parent / "../log/casmart_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



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
                # cursor = sql_conn.cursor()
                # query = f"select count(*) count from Alfa_List"
                # cursor.execute(query)
                # for count in cursor:
                #     print(f"the count is {count}")
                # cursor.close()

                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                sku_link = pd.read_sql("select id as id, ifnull(sku, 'N/A') as sku, sku_link as sku_link from casmart_list "
                                       "where assigned=0 and retrieved=0 order by RAND() limit 3000;", sql_conn)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                # sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku_link":["https://www.alfa.com/zh-cn/catalog/056166/"]})
                # print(sku_link)
                ### sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                if len(sku_link) != 0:
                   with sql_conn.cursor() as cursor:
                       update_sql = "Update casmart_list set assigned=1 where ID in (" + ', '.join(
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
                        with open(Path(__file__).parent / "../log/casmart_SKU_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        raise
        with open(Path(__file__).parent / "../log/casmart_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
#
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


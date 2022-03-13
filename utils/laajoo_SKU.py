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
import mysql.connector
from mysql.connector import errorcode
import json
import random
from sqlalchemy import create_engine

def get_Alfa_sku(url, link_id, sku, browser, conn):
    # res = requests.get(url, verify=False)
    try:
        with conn.cursor() as cursor:


            browser.get(url)

            # time.sleep(5)
            time.sleep(int(random.random() * 10))
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
            # res = requests.get(url)
            # print(res.headers)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            if soup.find_all('title') != []:
                title_text = str(soup.find_all('title')[0].text).strip()

            if title_text == '404 - 找不到文件或目录。':
                # No product
                print(f'link_id: {link_id}, sku: {sku}, url: {url} does not exist. Marking as 404.')
                with conn.cursor() as cursor_update:
                    update_sql = f"Update laajoo_list set Flag_404=1 where ID = {link_id};"

                    cursor_update.execute(update_sql)
                    conn.commit()
                # pass
            # print(soup.prettify())
            # print(soup.prettify())
            else:
                # pass
                for main_all in soup.find_all('div', {"class":"kj-productinfo-summaryrow"}):
                    for main_info in main_all.find_all('div', {'class':'col-lg-8 col-md-7 col-sm-7 col-xs-12 kj_promcxx'}):
                        if main_info.find_all('h1') != []:
                            name_cn = str(soup.find_all('h1')[0].text).strip().replace("'", "''")

                        for main_text in main_info.find_all('div', {"class":"table-responsive kj_cplb"}):
                            for main_table in main_text.find_all('tbody'):
                                for tds in main_table.find_all('td'):
                                    if tds != []:
                                        main_td.append(str(tds.text).strip().replace("'", "''"))
                                # print(main_td)
                                if len(main_td) % 2 == 0:
                                    for info_name, info_value in zip(main_td[0::2], main_td[1::2]):

                                        if info_name == "CAS号":
                                            cas = info_value
                                        elif info_name == "分子量":
                                            mm = info_value
                                        elif info_name == "分子式":
                                            formula = info_value
                                        elif info_name == "货号":
                                            sku = info_value
                                        elif info_name == "产品分类":
                                            category = info_value
                                        elif info_name == "英文名":
                                            name_en = info_value
                                        elif info_name == "别名":
                                            synonym_cn = info_value
                                        else:
                                            print(str(info_name), ': ', str(info_value))
                                else:
                                    print(f'error in the infos: {main_td}')

                        #     if main_text.find_all('h4') != []:
                        #         name_cn = str(main_text.find_all('h4')[0].text).replace('中文名：', '').strip().replace("'", "''")
                        #     for main_text_infos in main_text.find_all('p', {"class": "col333 mt10 fb"}):
                        #         if main_text_infos !=[]:
                        #             cas_sku = str(main_text_infos.text).strip().replace("'", "''")
                        #
                        #         if main_text_infos.find_all("span") != []:
                        #             sku = str(main_text.find_all('span')[0].text).strip().replace("'", "''")
                        #             cas = str(cas_sku.replace(sku, '').replace('CAS No.:', '')).strip()
                        #             sku = str(sku.replace('产品编号：', '')).strip()
                        # for infos_area in main_info.find_all('div', {"id":"001"}):
                        #     for infos_tbl in infos_area.find_all('table', {'class':'mt10'}):
                        #         for infos in infos_tbl.find_all('tbody'):
                        #             if infos.find_all("td") != []:
                        #                 infos_len = len(infos.find_all("td"))
                        #                 # if infos_len > 0:
                        #                 #     synonym_cn = str(infos.find_all("dd")[0].text).strip().replace("'", "''")
                        #                 # if infos_len > 1:
                        #                 #     sku = str(infos.find_all("dd")[1].text).strip().replace("'", "''")
                        #                 # if infos_len > 2:
                        #                 #     name_en = str(infos.find_all("dd")[2].text).strip().replace("'", "''")
                        #                 if infos_len > 1:
                        #                     formula = str(infos.find_all("td")[1].text).strip().replace("'", "''")
                        #                 # if infos_len > 4:
                        #                 #     synonym_en = str(infos.find_all("dd")[4].text).strip().replace("'", "''")
                        #                 if infos_len > 5:
                        #                     mm = str(infos.find_all("td")[5].text).strip().replace("'", "''")
                        #                 # if infos_len > 6:
                        #                 #     cas = str(infos.find_all("dd")[6].text).strip().replace("'", "''")
                        #                 if infos_len > 9:
                        #                     purity = str(infos.find_all("td")[9].text).strip().replace("'", "''")

                for main_stock in main_all.find_all('div', {"id":"procg"}):
                    for stock_table in main_stock.find_all('div', {'id': 'progoodslist'}):
                        for stock_body in main_stock.find_all('tbody'):
                            if stock_body.find_all('tr') != []:
                                for stock_line in stock_body.find_all('tr'):
                                    if stock_line.find_all("td") != []:
                                        stock_len = len(stock_line.find_all("td"))
                                        if stock_len > 0:
                                            sku_size = str(stock_line.find_all("td")[0].text).strip().replace("'", "''")
                                        if stock_len > 1:
                                            purity = str(stock_line.find_all("td")[1].text).strip().replace("'", "''")
                                        if stock_len > 2:
                                            brand = str(stock_line.find_all("td")[2].text).strip().replace("'", "''")
                                        if stock_len > 3:
                                            size = str(stock_line.find_all("td")[3].text).strip().replace("'", "''")
                                        if stock_len > 4:
                                            market_price_dirty = str(stock_line.find_all("td")[4].text).strip().replace("'", "''")
                                            market_price = market_price_dirty
                                            # print(market_price_dirty)
                                        if stock_len > 5:
                                            promo_price = str(stock_line.find_all("td")[5].text).strip().replace("'", "''")
                                        if stock_len > 6:
                                            vip_price = str(stock_line.find_all("td")[6].text).strip().replace("'", "''")
                                        if stock_len > 7:
                                            stock = str(stock_line.find_all("td")[7].text).strip().replace("'", "''")




                                        try:
                                            cursor.execute(f"insert into laajoo_sku ("
                                                           f"cas, size, market_price, promo_price, vip_price,"
                                                           f" stock, name_cn, name_en, list_id, url, sku, "
                                                           f"purity,  formula, molecular_mass, sku_size, brand,"
                                                           f"synonym_cn, category) values ("
                                                           f"'{cas}', '{size}', '{market_price}', '{promo_price}', '{vip_price}', "
                                                           f"'{stock}', '{name_cn}', '{name_en}', "
                                                           f"'{link_id}', '{url}', '{sku}', '{purity}',  "
                                                           f" '{formula}', '{mm}', '{sku_size}', '{brand}', "
                                                           f"'{synonym_cn}', '{category}');")
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
                    update_sql = f"Update laajoo_list set Retrieved=1 where ID = {link_id};"

                    cursor_update.execute(update_sql)
                    conn.commit()




    except Exception as e:
        # raise
        with open(Path(__file__).parent / "../log/laajoo_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")
        pass



def run_alfa():
    try:
        fireFoxOptions = Options()
        fireFoxOptions.headless = True
        fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver")
        # fireFoxService=Service(Path(__file__).parent / "../driver/geckodriver.exe")
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
                # cursor = sql_conn.cursor()
                # query = f"select count(*) count from Alfa_List"
                # cursor.execute(query)
                # for count in cursor:
                #     print(f"the count is {count}")
                # cursor.close()

                # sku = pd.DataFrame(["B2349", "B2351"], columns=["SKU"])
                sku_link = pd.read_sql("select ID as id, ifnull(SKU, 'N/A') as sku, SKU_Link as sku_link from laajoo_list "
                                       "where Assigned=0 and Retrieved=0 and Flag_404=0 order by RAND() limit 10;", sql_conn_df)
                # sku_link = pd.DataFrame({"id":sku["id"], "sku_link":"https://www.tcichemicals.com/US/en/p/" + sku["SKU"]}, columns=["id","sku_link"])
                # sku_link = pd.read_csv(Path(__file__).parent / "../Crawler_Website_Link/Alfa_Cat.csv", header=None)
                # sku_link.rename(columns={0:"sku_link"}, inplace=True)
                # sku_link["id"] = range(1, len(sku_link) + 1)

                # sku_link = pd.DataFrame({"id":[1], "sku_link":["https://www.alfa.com/zh-cn/catalog/056166/"]})
                print(sku_link)
                ### sku_link = pd.read_sql("select id, isnull(url, 'N/A') as sku_link from alfa_list", sql_conn)
                if len(sku_link) != 0:
                   with sql_conn.cursor() as cursor:
                       update_sql = "Update laajoo_list set Assigned=1 where ID in (" + ', '.join(
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
                        with open(Path(__file__).parent / "../log/laajoo_SKU_error.csv", "a") as f:
                            f.write(f"{datetime.now()}, Error when processing: ,{row.id}, {url}, error: {str(e)}")
                            f.write("\n")
                        pass
    except Exception as e:
        # raise
        with open(Path(__file__).parent / "../log/laajoo_SKU_error.csv", "a") as f:
            f.write(str(e))
            f.write("\n")

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

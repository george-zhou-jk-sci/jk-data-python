from os import path
import requests
from bs4 import BeautifulSoup
import pandas as pd
# import pyodbc
import numpy as np
import math
#from Get_Abs_URL import get_abstract_url
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
import sys



def total_items(URL):
    # browser = webdriver.Firefox(path.dirname(__file__) + "/Driver")
    fireFoxOptions = Options()
    fireFoxOptions.headless = True
    # fireFoxOptions.headless = False
    fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")
    with webdriver.Firefox(
            # executable_path=Path(__file__).parent / "driver/geckodriver.exe",
            service=fireFoxService,
            options=fireFoxOptions) as browser:
    # browser = webdriver.Firefox()
        browser.get(URL)
        for elem in browser.find_elements_by_xpath('.//span[@class = "result__count"]'):
            number=int(elem.text)
        # browser.close()
        return number

def get_search_url(URL):
    # select by value
    #select.select_by_value('1')
    #driver.find_element_by_xpath("//button[id ='advanced-search-btn']").click()
    # browser = webdriver.Firefox(path.dirname(__file__) + "/Driver")
    # browser = webdriver.Firefox()
    fireFoxOptions = Options()
    fireFoxOptions.headless = True
    # fireFoxOptions.headless = False
    fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")
    with webdriver.Firefox(
            # executable_path=Path(__file__).parent / "driver/geckodriver.exe",
            service=fireFoxService,
            options=fireFoxOptions) as browser:
        browser.get(URL)
        # save current page url
        current_url = browser.current_url

        # initiate page transition, e.g.:
        browser.find_element_by_id("advanced-search-btn").submit()

        # wait for URL to change with 15 seconds timeout
        WebDriverWait(browser, 15).until(EC.url_changes(current_url))

        # print new URL
        new_url=browser.current_url
        # browser.close()
        return new_url



def get_abstract_url(sql_conn, Abstract_ID, URL):
    fireFoxOptions = Options()
    fireFoxOptions.headless = True
    # fireFoxOptions.headless = False
    fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")
    with webdriver.Firefox(
            # executable_path=Path(__file__).parent / "driver/geckodriver.exe",
            service=fireFoxService,
            options=fireFoxOptions) as geckodriver:

        with sql_conn.cursor() as cursor:

            issue_item_result=pd.DataFrame()



            # page = requests.get(URL)
            #
            # soup = BeautifulSoup(page.content, 'html.parser')
            geckodriver.get(URL)
            html = geckodriver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            total_result=0
            if not soup.find('span', class_='result__count') == None:
                total_result = int(soup.find('span', class_='result__count').text.replace("\'", "\'\'"))
            if total_result==0:
                print('nothing')
            elif total_result>2000:
                total_page = 20
                # total_page=total_result/100
                for i in range(0, total_page):

                    page_url = URL + "&pageSize=100&startPage=" + str(i)

                    # item_page = requests.get(page_url)
                    # item_soup = BeautifulSoup(item_page.content, 'html.parser')
                    geckodriver.get(URL)
                    item_html = geckodriver.page_source
                    item_soup = BeautifulSoup(item_html, 'html.parser')
                    issue_item_elems = item_soup.find_all('div', class_='issue-item clearfix')
                    for issue_item_elem in issue_item_elems:
                        abs_href = issue_item_elem.find('a', {'title': ['Abstract', 'First Page', 'Full text']})
                        if not abs_href == None:
                            if not abs_href['href'][:4] == "http" or not abs_href['href'][:4] == "Http":
                                issue_item_result = issue_item_result.append(pd.Series(
                                    [str(Abstract_ID), "https://pubs.acs.org" + abs_href['href'].replace("\'", "\'\'")]).astype(
                                    str), ignore_index=True)
                            else:
                                issue_item_result = issue_item_result.append(
                                    pd.Series([str(Abstract_ID), abs_href['href'].replace("\'", "\'\'")]).astype(str),
                                    ignore_index=True)

            elif total_result<=20:
                page_url = URL

                # item_page = requests.get(page_url)
                # item_soup = BeautifulSoup(item_page.content, 'html.parser')
                geckodriver.get(URL)
                item_html = geckodriver.page_source
                item_soup = BeautifulSoup(item_html, 'html.parser')
                issue_item_elems = item_soup.find_all('div', class_='issue-item clearfix')
                for issue_item_elem in issue_item_elems:
                    abs_href = issue_item_elem.find('a', {'title': ['Abstract', 'First Page', 'Full text']})
                    if not abs_href == None:
                        if not abs_href['href'][:4] == "http" or not abs_href['href'][:4] == "Http":
                            issue_item_result = issue_item_result.append(pd.Series(
                                [str(Abstract_ID), "https://pubs.acs.org" + abs_href['href'].replace("\'", "\'\'")]).astype(str),
                                                                         ignore_index=True)
                        else:
                            issue_item_result = issue_item_result.append(
                                pd.Series([str(Abstract_ID), abs_href['href'].replace("\'", "\'\'")]).astype(str),
                                ignore_index=True)

            elif total_result<=100:
                page_url = URL + "&pageSize=100&startPage=0"

                # item_page = requests.get(page_url)
                # item_soup = BeautifulSoup(item_page.content, 'html.parser')
                geckodriver.get(URL)
                item_html = geckodriver.page_source
                item_soup = BeautifulSoup(item_html, 'html.parser')
                issue_item_elems = item_soup.find_all('div', class_='issue-item clearfix')
                for issue_item_elem in issue_item_elems:
                    abs_href = issue_item_elem.find('a', {'title': ['Abstract', 'First Page', 'Full text']})
                    if not abs_href == None:
                        if not abs_href['href'][:4] == "http" or not abs_href['href'][:4] == "Http":
                            issue_item_result = issue_item_result.append(pd.Series(
                                [str(Abstract_ID), "https://pubs.acs.org" + abs_href['href'].replace("\'", "\'\'")]).astype(str), ignore_index=True)
                        else:
                            issue_item_result = issue_item_result.append(
                                pd.Series([str(Abstract_ID), abs_href['href'].replace("\'", "\'\'")]).astype(str), ignore_index=True)

            else:
                total_page=math.ceil(float(total_result)/100)
                #total_page=total_result/100
                for i in range(0,total_page):

                    page_url=URL+"&pageSize=100&startPage="+str(i)

                    # item_page=requests.get(page_url)
                    # item_soup=BeautifulSoup(item_page.content, 'html.parser')
                    geckodriver.get(URL)
                    item_html = geckodriver.page_source
                    item_soup = BeautifulSoup(item_html, 'html.parser')
                    issue_item_elems = item_soup.find_all('div', class_='issue-item clearfix')
                    for issue_item_elem in issue_item_elems:
                        abs_href= issue_item_elem.find('a', {'title':['Abstract', 'First Page', 'Full text']})
                        if not abs_href==None:
                            if not abs_href['href'][:4] == "http" or not abs_href['href'][:4]=="Http":
                                issue_item_result = issue_item_result.append(pd.Series([str(Abstract_ID), "https://pubs.acs.org"+abs_href['href'].replace("\'", "\'\'")]).astype(str), ignore_index=True)
                            else:
                                issue_item_result = issue_item_result.append(pd.Series([str(Abstract_ID), abs_href['href'].replace("\'", "\'\'")]).astype(str), ignore_index=True)

            if not total_result==0:
                issue_item_result.columns = ['Subj_ID', 'Href']

                for index, row in issue_item_result.iterrows():
                    cursor.execute(
                        f"INSERT INTO abstract_url (Subject_Index_ID, Href) values ({int(row.Subj_ID)},'{str(row.Href)}')")
                sql_conn.commit()



def get_issue_item_number(URL):
    # page = requests.get(URL)
    total_result = 0
    fireFoxOptions = Options()
    fireFoxOptions.headless = True
    # fireFoxOptions.headless = False
    fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")
    with webdriver.Firefox(
            # executable_path=Path(__file__).parent / "driver/geckodriver.exe",
            service=fireFoxService,
            options=fireFoxOptions) as geckodriver:
        geckodriver.get(URL)
        html = geckodriver.page_source
        soup = BeautifulSoup(html, 'html.parser')


        if not soup.find('span', class_='result__count') == None:
            total_result = int(soup.find('span', class_='result__count').text.replace("\'", "\'\'"))
    return total_result



def get_item_number(sql_conn, Subj_ID, search_url, get_start_year=1753, get_end_year=datetime.today().year):
    if get_start_year > get_end_year or get_end_year < 1991:
        print('start end year error.')
        return None
    else:
        with sql_conn.cursor() as cursor:
            qry = "insert into item_number (Subj_ID, Items, from_year, to_year, Link) values ("
            if get_start_year < 1991:
                start_year=1753
                end_year=1900

                full_url=search_url+"&Earliest=&AfterMonth=&AfterYear=" +str(start_year) + "&BeforeMonth=&BeforeYear="+str(end_year)

                qry = qry + Subj_ID + ", " + str(
                    get_issue_item_number( full_url)) + ", '" + str(start_year) + "', '" + str(end_year) + "', '" + search_url+ "'), ("


                start_year = 1901
                end_year = 1950

                full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(start_year) + "&BeforeMonth=&BeforeYear=" + str(
                    end_year)

                qry = qry  + Subj_ID + ", " + str(
                    get_issue_item_number( full_url)) + ", '" + str(start_year) + "', '" + str(end_year) + "', '" + search_url+ "'), ("


                start_year = 1951
                end_year = 1970

                full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(start_year) + "&BeforeMonth=&BeforeYear=" + str(
                    end_year)

                qry = qry  + Subj_ID + ", " + str(
                    get_issue_item_number( full_url)) + ", '" + str(start_year) + "', '" + str(end_year) + "', '" + search_url+ "'), ("


                start_year = 1971
                end_year = 1980

                full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(start_year) + "&BeforeMonth=&BeforeYear=" + str(
                    end_year)

                qry = qry + Subj_ID + ", " + str(
                    get_issue_item_number( full_url)) + ", '" + str(start_year) + "', '" + str(end_year) + "', '" + search_url+ "'), ("


                start_year = 1981
                end_year = 1990

                full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(start_year) + "&BeforeMonth=&BeforeYear=" + str(
                    end_year)

                qry = qry + Subj_ID + ", " + str(
                    get_issue_item_number( full_url)) + ", '" + str(start_year) + "', '" + str(end_year) + "', '" + search_url


            if get_start_year < 1991:
                for k in range(1991, get_end_year + 1):
                    start_year = k
                    end_year = k

                    full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(
                        start_year) + "&BeforeMonth=&BeforeYear=" + str(
                        end_year)

                    qry = qry + "'), (" + Subj_ID + ", " + str(
                        get_issue_item_number(full_url)) + ", '" + str(start_year) + "', '" + str(
                        end_year) + "', '" + search_url
                qry = qry + "')"
                cursor.execute(qry)
                sql_conn.commit()
            elif get_start_year == get_end_year:

                full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(
                    get_start_year) + "&BeforeMonth=&BeforeYear=" + str(
                    get_start_year)

                qry = qry + Subj_ID + ", " + str(
                    get_issue_item_number(full_url)) + ", '" + str(get_start_year) + "', '" + str(
                    get_start_year) + "', '" + search_url
                qry = qry + "')"
                cursor.execute(qry)
                sql_conn.commit()
            else:
                full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(
                    get_start_year) + "&BeforeMonth=&BeforeYear=" + str(
                    get_start_year)

                qry = qry + Subj_ID + ", " + str(
                    get_issue_item_number(full_url)) + ", '" + str(get_start_year) + "', '" + str(
                    get_start_year) + "', '" + search_url
                for k in range(get_start_year+1,get_end_year+1):
                    start_year = k
                    end_year = k

                    full_url = search_url + "&Earliest=&AfterMonth=&AfterYear=" + str(start_year) + "&BeforeMonth=&BeforeYear=" + str(
                        end_year)

                    qry = qry + "'), (" + Subj_ID + ", " + str(
                        get_issue_item_number( full_url)) + ", '" + str(start_year) + "', '" + str(end_year) + "', '" + search_url
                qry = qry + "')"
                cursor.execute(qry)
                sql_conn.commit()




# sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=Georgetz.com,12154; DATABASE=ACS;   UID=datarw;   PWD=Toronto2020')
# sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=George-PC; DATABASE=ACS;   UID=datarw;   PWD=Toronto2020')
#Subj_ID="1"
#URL="https://pubs.acs.org/topic/acs-lex2/724?seriesKey=&tagCode=&target=topical-index-search&TaxPubType=tags-articles-chapters"
#driver = webdriver.Firefox(path.dirname(__file__)+"/Driver")
# driver = webdriver.Firefox()
if len(sys.argv) != 2:
    print('expect only get current year data')
    get_start_year = datetime.today().year
    get_end_year = datetime.today().year
else:
    get_start_year = sys.argv[0]
    get_end_year = sys.argv[1]
fireFoxOptions = Options()
fireFoxOptions.headless = True
# fireFoxOptions.headless = False
fireFoxService = Service(Path(__file__).parent / "../driver/geckodriver.exe")
with open(Path(__file__).parent / "config/mysql_config.json", 'r') as config_file:

    # parse file
    config = json.loads(config_file.read())


with webdriver.Firefox(
        # executable_path=Path(__file__).parent / "driver/geckodriver.exe",
        service=fireFoxService,
        options=fireFoxOptions) as driver:

    with mysql.connector.connect(**config) as sql_conn:

        # cursor = sql_conn.cursor()
        query='SELECT Topical_Index_ID, Link FROM subject_list where Assigned=0 and Retrieved=0 order by RAND() limit 200;'

        subject_list=pd.read_sql(query, sql_conn)
        if not len(subject_list)==0:
            with sql_conn.cursor() as cursor:
                update_sql = "Update subject_list set Assigned=1 where Topical_Index_ID in (" + ', '.join(list(map(str, subject_list.iloc[:, 0].tolist()))) + ")"

                cursor.execute(update_sql)
                sql_conn.commit()


            for i in range(0, len(subject_list)):

                Subj_ID=subject_list.iloc[i, 0]
                URL=subject_list.iloc[i, 1]



                driver.get(URL)
                assert "ACS" in driver.title

                # total_item=total_items(URL)

                # if total_item<=2000:
                #     get_abstract_url(sql_conn, Subj_ID, URL)
                # elif total_item>2000:
                if True: # try all item
                    search_url=get_search_url(URL)
                    #print(new_url)
                    #url type &Earliest=&AfterMonth=&AfterYear=2019&BeforeMonth=&BeforeYear=2020
                    #url example : https://pubs.acs.org/action/doSearch?field1=AllField&text1=&ConceptID=&ConceptID=&ConceptID%5B%5D=291163&publication=&accessType=allContent&Earliest=&Earliest=&AfterMonth=&AfterYear=2019&BeforeMonth=&BeforeYear=2020
                    get_item_number(sql_conn,str(Subj_ID),search_url, get_start_year, get_end_year)
                    with sql_conn.cursor() as cursor:

                        query='SELECT Index_ID, Items, from_year ,to_year, Link, Subj_ID  FROM item_number ' \
                              'where Assigned=0 and Retrieved=0 and Subj_ID=' + str(Subj_ID) +';'

                        item_list=pd.read_sql(query, sql_conn)

                        update_sql = "Update item_number set Assigned=1 where Index_ID in (" + ', '.join(list(map(str, item_list.iloc[:, 0].tolist()))) + ")"

                        cursor.execute(update_sql)
                        sql_conn.commit()

                    for l in range(0, len(item_list)):

                        item_Subj_ID=item_list.iloc[l, 5]
                        item_URL=item_list.iloc[l, 4]
                        item_Number=item_list.iloc[l, 1]
                        from_year=item_list.iloc[l, 2]
                        to_year=item_list.iloc[l, 3]
                        if not item_Number==0:
                            if item_Number<=2000:
                                #execute all
                                get_abstract_url(sql_conn,item_Subj_ID, item_URL + "&Earliest=&AfterMonth=&AfterYear=" + from_year + "&BeforeMonth=&BeforeYear=" + to_year)
                            elif from_year==to_year and item_Number>2000:
                                # execute every month;
                                for month in range(1,13):
                                    get_abstract_url(sql_conn, item_Subj_ID,
                                                 item_URL + "&Earliest=&AfterMonth=" + str(month) + "&AfterYear=" + from_year + "&BeforeMonth=" + str(month) + "&BeforeYear=" + to_year)
                            else:
                                #only execute first 2000
                                get_abstract_url(sql_conn,item_Subj_ID, item_URL + "&Earliest=&AfterMonth=&AfterYear=" + from_year + "&BeforeMonth=&BeforeYear=" + to_year)

                    update_sql="Update item_number set Retrieved=1 where Index_ID in (" + ', '.join(list(map(str, item_list.iloc[:, 0].tolist()))) + ")"
                    with sql_conn.cursor() as cursor:
                        cursor.execute(update_sql)
                        sql_conn.commit()
                else:
                    print("Something is wrong")

                update_done_sql = "Update subject_list set Retrieved=1 where Topical_Index_ID in (" + ', '.join(list(map(str, subject_list.iloc[:,0].tolist()))) + ")"
                with sql_conn.cursor() as cursor:
                    cursor.execute(update_done_sql)
                    sql_conn.commit()

            # cursor.close()

                #get_abstract_url(sql_conn,"","")
        else:
            print('nothing to run')

        # driver.close()
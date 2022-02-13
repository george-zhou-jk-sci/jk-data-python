
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

with open(Path(__file__).parent / "../config/mysql_config_acs.json", 'r') as config_file:
    # parse file
    config = json.loads(config_file.read())
# print(config)
with mysql.connector.connect(**config) as sql_conn:
# sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=Georgetz.com,12154; DATABASE=ACS;   UID=datarw;   PWD=Toronto2020')
# sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=George-PC; DATABASE=ACS;   UID=datarw;   PWD=Toronto2020')
    with sql_conn.cursor() as cursor:
        #
        #
        #
        query='SELECT Abstract_ID, Href, subject_index_id FROM abstract_url where Assigned=0 and Retrieved=0 order by RAND() limit 10;'
        #
        abstract_list=pd.read_sql(query, sql_conn)
        #
        update_sql = "Update abstract_url set Assigned=1 where Abstract_ID in (" + ', '.join(list(map(str, abstract_list.iloc[:,0].tolist()))) + ")"
        #
        cursor.execute(update_sql)
        sql_conn.commit()
        #
        qry="insert into article_info (Abstract_ID ,Title ,Cite ,Pub_Date ,Doi_URL ,Views ,Altmetric ,Citation ,Subject" \
            " ,Abstract, subject_index_id, pub_dt, pub_yr) values ('"
        author_result=pd.DataFrame()
        for i in range(0,len(abstract_list)):

            Abstract_ID=abstract_list.iloc[i,0]
            subject_index_id=abstract_list.iloc[i,2]

            title=''
            cite=''
            pub_date=''
            doi_url=''
            views=''
            altmetric=''
            citations=''
            subject=''
            abstract=''


            URL=abstract_list.iloc[i,1].replace(" ","")
            # URL='https://pubs.acs.org/doi/abs/10.1021/acs.macromol.1c02005'
            #URL='https://pubs.acs.org/doi/10.1021/cm980074y'
            print(URL)
            page=requests.get(URL)
            time.sleep(int(random.random()*10))
            soup=BeautifulSoup(page.content, 'html.parser')
            #tag=soup.new_tag("i")
            #tag.string="Python"

            #soup.d.string.insert_after(tag)

            if not soup.find('span', class_='hlFld-Title') == None:
                title=soup.find('span', class_='hlFld-Title').text.replace("\'", "\'\'")
            if not soup.find_all('ul', class_='loa') is None:
                # author_elems = soup.find_all('span', class_='hlFld-ContribAuthor')
                for author_block in soup.find_all('ul', class_='loa'):
                    if not author_block is None and not author_block is soup.find_all('ul', {'class':['rlist--inline']})[0]:

                    # if not soup.find('div', 'loa-info-name') == None:
                    #     for author_elem in author_elems:
                    #         #    print(author_elem.replace_with(" "), end='\n'*2)
                    #         #   print(author_elem, end='\n'*2)
                    #         temp = [str(Abstract_ID)]
                    #
                    #         author_infos = author_elem.find_all('div',
                    #                                             {'class': ['loa-info-name', 'loa-info-affiliations-info']})
                    #         for author_info in author_infos:
                    #             if not author_info.text == None:
                    #                 temp.append(str(author_info.text.replace("\'", "\'\'")))
                    #             else:
                    #                 temp.append(" ")
                    #         #   author_elem.string.insert_after(tag)
                    #         #   temp.append(author_elem.text)
                    #         #   print(author_infos, end='\n'*2)
                    #         author_result = author_result.append(pd.Series(temp).astype(str), ignore_index=True)
                    # else:
                        for author_elems in author_block.find_all('li'):
                            # address_elems = author_elems.find_all('div', class_='loa-info-affiliations')
                            # result=result.append([1])
                            for author_elem in author_elems:
                                address_elems = author_elem.find_all('div', {'class':['loa-info-affiliations']})
                                temp = [str(Abstract_ID)]
                                if not author_elem.find_all('span', class_='hlFld-ContribAuthor') == None and len(author_elem.find_all('span', class_='hlFld-ContribAuthor')) > 0:
                                    temp.append(str(author_elem.find_all('span', class_='hlFld-ContribAuthor')[0].text.replace("\'", "\'\'")))
                                # elif not address_elems.find_all('div', class_='loa-info-name') is None:
                                #     temp.append(str(address_elems.find_all('div', class_='loa-info-name')[0].text.replace("\'", "\'\'")))
                                else:
                                    temp.append(" ")

                                for address_elem in address_elems:
                                    for address_elem_each in address_elem.find_all('div', {'class':'loa-info-affiliations-info'}):
                                        if not address_elem_each.text == None:
                                            temp.append(str(address_elem_each.text.replace("\'", "\'\'")))
                                        else:
                                            temp.append(" ")
                                author_result = author_result.append(pd.Series(temp).astype(str), ignore_index=True)

            else:
                author_elems = soup.find_all('span', class_='hlFld-ContribAuthor')
                # author_elems = soup.find_all('ul', class_='loa')
                if not soup.find('div', 'loa-info-name')== None:
                    for author_elem in author_elems:
                    #    print(author_elem.replace_with(" "), end='\n'*2)
                     #   print(author_elem, end='\n'*2)
                        temp=[str(Abstract_ID)]

                        author_infos=author_elem.find_all('div', {'class':['loa-info-name', 'loa-info-affiliations-info']})
                        for author_info in author_infos:
                            if not author_info.text==None:
                                temp.append(str(author_info.text.replace("\'", "\'\'")))
                            else:
                                temp.append(" ")
                     #   author_elem.string.insert_after(tag)
                     #   temp.append(author_elem.text)
                     #   print(author_infos, end='\n'*2)
                        author_result=author_result.append(pd.Series(temp).astype(str), ignore_index=True)
                else:
                    address_elems = soup.find_all('span', class_='aff-text')
                    #result=result.append([1])
                    for author_elem in author_elems:
                        temp=[str(Abstract_ID)]
                        if not author_elem.a == None:
                            temp.append(str(author_elem.a.text.replace("\'", "\'\'")))
                        else:
                            temp.append(" ")

                        for address_elem in address_elems:
                            if not address_elem.text == None:
                                temp.append(str(address_elem.text.replace("\'", "\'\'")))
                            else:
                                temp.append(" ")
                        author_result=author_result.append(pd.Series(temp).astype(str), ignore_index=True)




            #print(author_elems)
            #print(author_result)
            #print(len(temp))
            if not soup.find('div', class_='article_header-cite-this') == None:
                cite=soup.find('div', class_='article_header-cite-this').text.replace("\'", "\'\'")
            if not soup.find('span', class_='pub-date-value') == None:
                pub_date=soup.find('span', class_='pub-date-value').text.replace("\'", "\'\'")
            if not soup.find('div', class_='article_header-doiurl') == None:
                doi_url=soup.find('div', class_='article_header-doiurl').text.replace("\'", "\'\'")
            if not soup.find('div', class_='articleMetrics-val') == None:
                views=soup.find('div', class_='articleMetrics-val').text.replace("\'", "\'\'")
            if not soup.find('div', class_='articleMetrics-val articleMetrics_score') == None:
                altmetric=soup.find('div', class_='articleMetrics-val articleMetrics_score').text.replace("\'", "\'\'")
            if not soup.find('div', class_='articleMetrics_count') == None:
                citations=soup.find('div', class_='articleMetrics_count').text[9:].replace("\'", "\'\'")
            if not soup.find('ul', class_='rlist--inline loa') == None:
                subject=soup.find('ul', class_='rlist--inline loa').text.replace("\'", "\'\'")
            if not soup.find('p', class_='articleBody_abstractText') == None:
                abstract=soup.find('p', class_='articleBody_abstractText').text.replace("\'", "\'\'")

            qry=qry + str(Abstract_ID) + "', '" + title + "', '" + cite + "', '" + pub_date + "', '" \
                + doi_url + "', '" + views + "', '" + altmetric + "', '" + citations + "', '" \
                + subject + "', '" + abstract + "', '" + str(subject_index_id) \
                + "', '" + str(parse(pub_date).date()) +  "', " + str(parse(pub_date).year) + "), ('"

        while True:
            if len(author_result.columns) < 8:
                author_result['col1'] = ' '
                author_result['col2'] = ' '
                author_result['col3'] = ' '
                author_result['col4'] = ' '
                author_result['col5'] = ' '
                author_result['col6'] = ' '
                author_result['col7'] = ' '
                author_result['col8'] = ' '
            elif len(author_result.columns) > 8:
                author_result.drop(author_result.columns[len(author_result.columns) - 1], axis='columns', inplace=True)
            if len(author_result.columns) == 8:
                break
        author_result.replace([np.inf, -np.inf], np.nan, inplace = True)
        author_result.fillna(value='')
        author_result.columns = ['ID', 'A', 'B', 'C', 'D', 'E', 'F', 'G']
        # print(abstract_list)
        for index, row in author_result.iterrows():
            cursor.execute(f"INSERT INTO author (Abstract_ID,Author, Author_Address_1, Author_Address_2, Author_Address_3, Author_Address_4, Author_Address_5, Author_Address_6) values"
                           f" ('{str(row.ID)}','{str(row.A)}','{str(row.B)}','{str(row.C)}','{str(row.D)}','{str(row.E)}','{str(row.F)}','{str(row.G)}')")
        sql_conn.commit()


        qry = qry[:len(qry)-4]
        cursor.execute(qry)
        sql_conn.commit()

        update_done_sql = "Update abstract_url set Retrieved=1 where Abstract_ID in (" + ', '.join(list(map(str, abstract_list.iloc[:,0].tolist()))) + ")"

        cursor.execute(update_done_sql)
        sql_conn.commit()
        #
        # cursor.close()
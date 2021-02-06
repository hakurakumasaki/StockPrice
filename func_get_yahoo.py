import requests
from bs4 import BeautifulSoup
from pathlib import Path
import urllib
import time
from pprint import pprint
import datetime
import sqlite3
import re
import csv
import datetime

oldest_date = '2019-01-01'

def get_price(ID,sy,sm,sd,ey,em,ed,start_date,end_date):
    #yahooから株価データを取得する際のページ送り回数を計算
    number_of_days = end_date - start_date
    #print('number_of_days:',number_of_days)
    page_count = int( (number_of_days.days * 5 / 7) // 20  +1)
    #print('page_count:',page_count)

    row_list = []

    for page_number in range(page_count):
        url = 'https://info.finance.yahoo.co.jp/history/?code={security_code}&sy={start_year}&sm={start_month}&sd={start_day}&ey={end_year}&em={end_month}&ed={end_day}&tm=d&p={page}'

        url = url.format(security_code=ID,start_year=sy,start_month=sm,start_day=sd,end_year=ey,end_month=em,end_day=ed,page=page_number+1)

        html = requests.get(url)
        time.sleep(1)

        soup = BeautifulSoup(html.content,'html.parser')
        data_section = soup.find(class_='padT12')

        rows = data_section.find_all('tr')
        row = []

        for tr in rows[1:]:
            td = tr.find_all('td')
            #td[0]:日付
            #td[1]:始値
            #td[2]:高音
            #td[3]:安値
            #td[4]:終値
            #td[5]:出来高
            #td[6]:調整後終値

            #日付を２桁揃えに調整
            adj_date = td[0].text
            y = re.search(r'.*年',adj_date).group(0)
            yy = y.replace('年','')

            m = re.search(r'.*月',adj_date).group(0)
            mm = int(m.replace(y,'').replace('月',''))
            mm = '{:02d}'.format(mm)

            d = re.search(r'.*日',adj_date).group(0)
            dd = d.replace(m,'').replace('日','')
            dd = int(dd.replace(y,'').replace(m,'').replace('日',''))
            dd = '{:02d}'.format(dd)

            adj_date = yy + '-' + mm + '-' + dd
            #株価データからコンマを除去（sqliteで文字列として認識しないように
            try :
                td_1 = float(td[1].text.replace(',',''))
                td_2 = float(td[2].text.replace(',',''))
                td_3 = td[3].text.replace(',','')
                td_4 = td[4].text.replace(',','')
                td_5 = td[5].text.replace(',','')
                td_6 = td[6].text.replace(',','')
            except:
                pass

            for k in [1,2,3,4,5,6]:
                try:
                    tdv = ('td_' + str(k))
                    tdv = float(td[k].text.replace(',',''))
                except:
                    pass

            #株価データを保存するリストに代入
            row = [adj_date,td_1,td_2,td_3,td_4,td_5,td_6]
            row_list.append(row)
            #print('row_list from get_price;',row_list)
    return row_list


def check_latest_date(ID_x):
    #DBに接続
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    table_name = ('stock_price_' + ID_x)
    technical_data_tbname = ('technical_' + ID_x)
    table_name_stockPrice = ('stock_price_' + ID_x)

    latest_date = 'SELECT date from {} ORDER BY {} DESC'.format(table_name_stockPrice,'date')

    d = cursor.execute(latest_date)

    s_date = d.fetchone()
    #print(type(s_date))

    if s_date == None:
        return oldest_date
    else:
        return s_date[0]
    #return s_date[0]


def update_db(sp_tb_name,list):
    #データベースに接続
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()

    j = len(list)
    for i in range(j):

        insert_data = """INSERT OR REPLACE INTO {} VALUES(?,?,?,?,?,?,?);""".format(sp_tb_name)

        data = (list[i][0],list[i][1],list[i][2],list[i][3],list[i][4],list[i][5],list[i][6])
        #print('data:',data)

        cursor.execute(insert_data,data)
    connection.commit()
    return (sp_tb_name)

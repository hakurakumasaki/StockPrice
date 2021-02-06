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
import func_get_yahoo

def check_latest_corpAction(table_name):#基本的には'historical_corporate_action'をtable_nameとして渡す
    #DBに格納されているコーポレートアクションを取得して、コーポレートアクションの一覧をリストで返却
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    latest_date = 'SELECT effective_date,security_code from {} ORDER BY {} DESC'.format(table_name,'effective_date')

    t = cursor.execute(latest_date)
    existing_corpAction_data = t.fetchall()
    return existing_corpAction_data

def check_splitAction(table_name,security_code):
    #DBに格納されているコーポレートアクションを取得して、株式分割のコーポレートアクションの一覧をリストで返却
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    splitAction = 'SELECT effective_date,security_code from {} WHERE security_code = {} AND type_action = "分割" ORDER BY {} DESC'.format(table_name,security_code,'effective_date')

    t = cursor.execute(splitAction)
    existing_splitAction_data = t.fetchall()
    return existing_splitAction_data

def update_corporate_action():
    #みんかぶからコーポレートアクションのデータを取得
    url = 'https://minkabu.jp/top/stock_news/?column=announced_on&order=desc&page={page_number}'
    url = url.format(page_number=1)

    html = requests.get(url)
    time.sleep(1)#データ取得が１ページのみなら不要だが、保守的に設定

    soup = BeautifulSoup(html.content,'html.parser')
    data_section = soup.find(class_='js_stock_news')
    rows = data_section.find_all('tr')

    data_list = []
    for tr in rows[1:]:
        td = tr.find_all('td')

        data_tuple = ()
        for data_r in td:
            data = data_r.text
            data_tuple = data_tuple + (data,)

        #企業名と証券コードを分離してリストを再作成
        ID = re.findall("(?<=\().+?(?=\))",data_tuple[3])[0]#最後の[0]はタプルを数字に変換するため
        company = re.sub("\([1-9]+\)",'',data_tuple[3])
        data_l = list(data_tuple)
        data_l.insert(4,ID)

        #日付の形式をYYYY m-ddに変換
        data_l[0] = data_l[0].replace('/','-')
        data_l[2] = data_l[2].replace('/','-')

        data_modified = [data_l[0],data_l[1],data_l[2],company,ID,data_l[5]]
        #（ID：プライマリーキーは自動設定）、発表日、アクションの種類、効力発生日、企業名、証券コード、分割比率
        data_list.append(data_modified)

    #DBに接続
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    table_name = ('historical_corporate_action')

    #コーポレートアクションの記録を格納するDBが無ければ作成する
    try:
        create_table = 'CREATE TABLE IF NOT EXISTS {} (ID integer primary key, announce_date date, type_action text not NULL, effective_date date not NULL,company_name text not NULL, security_code integer not NULL,split_ratio text)'.format(table_name)

        cursor.execute(create_table)

    except sqlite3.Error as e:
        print(e)

    existing_corpAction_data = check_latest_corpAction(table_name)

    #最新のコーポレートアクションをDBに追加する
    #[0]:Announcement datetime
    #[1]:type of type_action
    #[2]:Effective datetime
    #[3]:Company name
    #[4]:ID
    #[5]:split ratio
    insert_data = """INSERT OR REPLACE INTO {}(announce_date,type_action,effective_date,company_name,security_code,split_ratio) VALUES(?,?,?,?,?,?);""".format(table_name)

    #既にDBに登録されているコーポレートアクションを検索して、存在しないもののみ追加する
    for d in range(len(data_list)):
        data = [data_list[d][0],data_list[d][1],data_list[d][2],data_list[d][3],data_list[d][4],data_list[d][5]]
        #print('data:',data)
        dataForValidation = (data_list[d][2],int(data_list[d][4]))

        if dataForValidation in existing_corpAction_data:
            print('already exists')
        else:
            cursor.execute(insert_data,data)
            print('DB updated')
    connection.commit()

def main():

    #コーポレートアクションのデータを取得して未取得のデータのみDBに書き込み
    update_corporate_action()

if __name__ =='__main__':
    main()

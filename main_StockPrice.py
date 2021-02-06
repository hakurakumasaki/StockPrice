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
import check_corp_action
import func_initialize

def main():
    #DBの作成等、初回に必要な設定作業
    func_initialize.createDB()
    print("initialization stock price DB created")
    with open('security_code.csv') as f:
        h = next(csv.reader(f))
        for gyou in csv.reader(f):
            ID_x = gyou[0]+'T'
            table_name_stockPrice = ('stock_price_' + ID_x)
            func_initialize.create_table(ID_x)

    print("initialization table for stock price DB created")

    func_initialize.create_historical_corpActionDB()
    print("initialization historical corporate action DB created")
    #初回用の設定作業ここまで---#

    #----銘柄ごとの処理に入る前の共通処理----
    #コーポレートアクションの情報をDBに格納
    check_corp_action.update_corporate_action()
    print("corporate action information updated to DB")
    #---共通処理ここまで---#

    #銘柄ごとの処理---
    #DBから最新の株価の日付を取り出して、それ以降のデータを更新する
    with open('security_code.csv') as f:
        h = next(csv.reader(f))
        for gyou in csv.reader(f):
            ID = gyou[0] + '.T'
            ID_n = gyou[0]
            ID_x = gyou[0]+'T'
            technical_data_tbname = ('technical_' + ID_x)
            table_name_stockPrice = ('stock_price_' + ID_x)

            print(ID_x,' updating')

            #DBからデータを取得して、最新の日付を確認する
            start_date_str = func_get_yahoo.check_latest_date(ID_x)

            #yahooからデータを取得する日付の範囲を指定
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.date.today()

            #株式分割の有無を確認して、前回DB更新日以降に分割があれば過去のデータを取り直す
            split_action_data = check_corp_action.check_splitAction('historical_corporate_action',ID_n)

            try:
                latest_split_date = datetime.datetime.strptime(split_action_data[0][0],'%Y-%m-%d').date()
                if latest_split_date > start_date and latest_split_date < end_date:
                    print('need to override past data')
                    start_date = datetime.datetime.strptime('2019/1/1','%Y-%m-%d').date() #データを取得したい開始日付を記載
                    with open('corp_action.csv','a') as c:
                        output = [split_action_data[0][0],split_action_data[0][1]]
                        print(output, file=c)
            except:
                print('no corporate action. Just update new data.')

            sy = datetime.datetime.strftime(start_date,'%Y')
            sm = datetime.datetime.strftime(start_date,'%m')
            sd = datetime.datetime.strftime(start_date,'%d')
            ey = datetime.datetime.strftime(end_date, '%Y')
            em = datetime.datetime.strftime(end_date, '%m')
            ed = datetime.datetime.strftime(end_date, '%d')

            row_list = func_get_yahoo.get_price(ID,sy,sm,sd,ey,em,ed,start_date,end_date)#yahooからデータを取得
            #print(row_list)
            len_rowList = len(row_list)

            a = func_get_yahoo.update_db(table_name_stockPrice,row_list)
            print(a,' updated')
        #銘柄ごとの処理ここまで---#

if __name__ =='__main__':
    main()

#コーポレートアクションがあれば、過去に遡ってデータを取得

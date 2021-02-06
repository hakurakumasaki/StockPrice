#プログラムの初回実行時に必要なDB作成等を実施
'''
1. DB作成（DB名：historical_stock_price.sqlite3）
2. 株価テーブルの作成（security_code.csvの各銘柄を、テーブル名"stock_price_xxxxT"として作成）
3. コーポレートアクションの履歴を格納するDB/テーブルを作成
'''
import sqlite3


def createDB():
    #データベースに接続
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()


def create_table(ID_x):
    table_name = ('stock_price_' + ID_x)
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()

    #銘柄ごとにテーブルを作成（作成済みならスキップ）
    try:
        dbpath = 'historical_stock_price.sqlite3'
        connection = sqlite3.connect(dbpath)
    except sqlite3.Error as e:
        print(e)

    try:
        create_table = 'CREATE TABLE IF NOT EXISTS {} (date date primary key not NULL, start_price float, high_price float, low_price float, end_price float, volume float, adj_end_price float)'.format(table_name)

        cursor.execute(create_table)

    except sqlite3.Error as e:
        print(e)

    connection.commit()

def create_historical_corpActionDB():
    #コーポレートアクションの記録を格納するDBに接続
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    table_name = ('historical_corporate_action')

    #コーポレートアクションの記録を格納するテーブルが無ければ作成する
    try:
        create_table = 'CREATE TABLE IF NOT EXISTS {} (ID integer primary key, announce_date date, type_action text not NULL, effective_date date not NULL,company_name text not NULL, security_code integer not NULL,split_ratio text)'.format(table_name)

        cursor.execute(create_table)

    except sqlite3.Error as e:
        print(e)

    connection.commit()

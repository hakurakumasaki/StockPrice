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
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

Result_SD = []
sns.set()

#指数化した日経平均の標準偏差を求めておく。これより大きい（値動きの荒い）銘柄を探す
#日経平均のデータを指数化してnikkei_data_indexedに格納
df = pd.read_csv('nikkei_avg_2020.csv', usecols=['end_price'])#終値だけ抽出
nikkei_data = df.values
nikkei_data_indexed = nikkei_data / nikkei_data[0] * 100

#指数化した日経平均の標準偏差を算出
m_nikkei = np.mean(nikkei_data_indexed)
sd_nikkei = np.std(nikkei_data_indexed, ddof = 0)


#証券コードを取得し、'list_ID_x'に格納　->　security_code.csvを開きっぱなしにしなくて良い
list_ID_x = []
with open ('security_code.csv') as f:
    ID_list = f.readlines()
    for line in ID_list:
        list_ID_x.append(line)

#株価DBから銘柄ごとの株価を取得し、PandasのDataFrameに格納
#証券コードのみを取得
#i=0はラベルなので、i=1からスタートする
i = 1
while i < len(list_ID_x) - 1:
    ID = list_ID_x[i]
    ID = re.search(r'\d+',ID).group()
    ID_x = ID + 'T'

    #DBへ接続、株価データを取得
    #必要な変数を定義
    dbpath = 'historical_stock_price.sqlite3'
    connection = sqlite3.connect(dbpath)
    cursor = connection.cursor()
    table_name_stockPrice = ('stock_price_' + ID_x)

    #DBからのデータ取得
    price_data = '''SELECT date, adj_end_price from {} ORDER BY {} DESC'''.format(table_name_stockPrice,'date')
    p = cursor.execute(price_data)
    date_price = p.fetchall()

    #PandasのDataFrameに格納し、2020年のデータのみを抽出
    df = pd.DataFrame(date_price, columns = ["date",'price'])#DBに入っているデータを丸ごと出力
    df = df.query('date >= "2020-01-01"')#2020年のデータのみを抽出

    #NumpyのArrayにデータを格納
    p_data = df.loc[:,["price"]].values
    #print("p_data[0]:",p_data[len(p_data)-1])データの一番最後が一番古いデータであることを確認
    initial_value = p_data[len(p_data) -1]
    p_data_indexed = p_data / initial_value * 100
    mu = np.mean(p_data_indexed)
    sd = np.std(p_data_indexed, ddof = 0)

    #株価全体の標準偏差より大きい物（=値動きが荒い銘柄）を抽出
    if sd > sd_nikkei:
        #result = [ID, mu, sd, p_data]#いろいろ出力したかったらこちらを使う
        result = [ID]
        Result_SD.append(result)
    else:
        None
    #sns.histplot(p_data)
    #plt.show()実行してしまうと大量の描画が必要なので、この行は実行しないで！!!

    i += 1

#グラフ描画
#fig, axs = plt.subplots(figsize=(5,5))
#axs.set_xlabel('indexed price')
#axs.set_ylabel('days')
#sns.histplot(p_data_indexed, color = "blue", kde = True, )
#sns.histplot(nikkei_data_indexed, color = 'red', kde = True, )
#plt.show()

#2020年の株価で日経平均より大きな標準偏差（大きなVolatility）を持っていた銘柄を出力
print("list of stocks which had greater Standard Deviation than Nikkei225 in 2020:",Result_SD)

'''
わからなかったこと

DBからデータを抽出する時点で、2020年のデータのみを抽出する方法
SQL文に WHERE date>="2020-01-01"　を追加してみたけど、思った通り動作せず

'''

#2月の気温から標本を抽出。95%信頼区間で検定する。

import numpy as np
import scipy as sp
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import csv
from scipy import stats
import datetime
import random


pd.set_option('display.max_rows', None)
sns.set()


df = pd.read_csv('./data_weather.csv', index_col ='date')
df.index = pd.to_datetime(df.index, format = '%Y-%m-%d')#日付を0で桁埋めするフォーマット。
df = df.astype('float64')#データの型をstrからfloatに変換。

#2月のデータだけを抽出
df = df[df.index.month == 2]['low_temp']
#df = df['low_temp']#2月の最低気温だけをデータとして持つDataFrameを定義

#過去10年の2月の最低気温の分布を確認
mu_a = np.mean(df)#aはallの略
n_a = len(df)
dof_a = n_a -1
#sns.histplot(df_l, kde = True)
#plt.show()

#過去10年の2月の最低気温の中からランダムに10日分のデータを取得
df_s = df.sample(n=10)#sはsampleの略

#サンプルについて統計量を計算
n_s = len(df_s)
dof_s = n_s -1
mu_s = np.mean(df_s)
se_s = np.std(df_s, ddof = 1) / np.sqrt(n_s)
interval = stats.t.interval(alpha = 0.95, df = dof_s, loc = mu_s, scale = se_s)
print(interval)

#sns.histplot(df, kde=True)
#sns.histplot(df_s, kde = True)
#plt.show()

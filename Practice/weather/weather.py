import numpy as np
import scipy as sp
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import csv
from scipy import stats

i = 0
temp_diff = []
pd.set_option('display.max_rows', None)
comp_value = 0.5

#csvのデータをリストとして読み込み
with open('./data_weather.csv', 'r') as f:
    data = list(csv.reader(f))

#日次差分データをDataFrameに格納。
df = pd.DataFrame(data, columns = ['date', 'low_temp', 'high_temp', 'ave_temp'])
df = df.set_index('date').drop('date').astype('float64')#リスト先頭のラベルを行タイトルにし、当該データを削除。データの型をstrからfloatに変換。
df_diff = df.diff(1)#前日とのデータの差分を取得

#統計量を計算
high_temp_diff = df_diff['high_temp'].drop(('2011/1/1'))
mu = np.mean(high_temp_diff)
n = len(high_temp_diff)
DoF = n -1
sigma = np.std(high_temp_diff, ddof = 1)#正規分布を仮定。不偏分散の場合はstdではなくvar
se = sigma/np.sqrt(n)
t_value = (mu - comp_value) / se
alpha = stats.t.cdf(np.abs(t_value), df = DoF)
p_value = (1 - alpha) * 2
lo, up = stats.t.interval(alpha = 0.95, df = DoF, loc = mu, scale = se)

#print(high_temp_diff)
#high_temp_diff.to_csv('output.csv')
#統計量を出力して確認
print(high_temp_diff.head())
print("mu:",mu)
print("DoF:",DoF)
print("se:",se)
print("alpha:",alpha)
print('t_value:',t_value)
print("p_value:", p_value)
print("low:",lo)
print("up:",up)

'''
if p_value < 0.05:
    print('meaningfully different')
else:
    print('not meaningfully different')
'''

#グラフ描画　←　必要な時だけコメントアウトする
#sns.histplot(high_temp_diff, kde=True)
#plt.show()

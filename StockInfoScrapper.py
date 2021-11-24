import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request as urllib
import numpy as np
import pymysql
from datetime import datetime
import re

opener = urllib.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0')]
pageUrl = 'https://finance.naver.com/item/frgn.nhn?ode=024810'
req = requests.get(pageUrl, headers={'User-agent': 'Mozilla/5.0'})
response = opener.open(pageUrl)
df = pd.DataFrame()
df = df.append(pd.read_html(response, encoding='euc-kr')[2])
df = df.fillna(0)
df = df.rename(columns={'날짜': 'date', '기관': 'institution', '외국인': 'foreigner', '순매매량': 'buy'})
df = df[['date', 'institution', 'foreigner']]
return_df = pd.DataFrame(columns=['date', 'institution', 'foreigner'])
index = 0
for r in df.itertuples():
    if r[1] != 0:
        date = r[1]
        institution_tmp = r[2].replace('+', '')
        foreigner_tmp = r[3].replace('+', '')
        institution = np.int64(institution_tmp.replace(',', ''))
        foreigner = np.int64(foreigner_tmp.replace(',', ''))
        tmp_list = [date, institution, foreigner]
        a_series = pd.Series(tmp_list, index=return_df.columns)
        return_df = return_df.append(a_series, ignore_index=True)


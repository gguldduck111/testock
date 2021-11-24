import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from Investar import MarketDB

mk = MarketDB.MarketDB()
stocks = ['삼성전자','SK하이닉스','현대자동차','NAVER','에코프로','에코프로비엠']
df = pd.DataFrame()
for s in stocks:
    df[s] = mk.get_daily_price(s, '2020-01-01','2021-04-19')['close']

daily_ret = df.pct_change()
annual_ret = daily_ret.mean() * 252
daily_cov = daily_ret.cov()
annul_cov = daily_cov * 252

print(daily_ret)
print(annual_ret)

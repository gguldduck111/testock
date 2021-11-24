import re
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pymysql
# import sqlalchemy
# from sqlalchemy import create_engine
import pandas as pd


class CompanyInfo:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        # self.conn = pymysql.connect(host='localhost', user='root',
        #                             password='djajSK1@90#', db='stock', charset='utf8')

    def get_finstate_naver(self, code):
        URL = f"https://finance.naver.com/item/main.naver?code={code}"
        r = requests.get(URL)
        df = pd.read_html(r.text)[3]
        df.set_index(df.columns[0], inplace=True)
        df.index.rename('주요재무정보', inplace=True)
        df.columns = df.columns.droplevel(2)
        annual_date = pd.DataFrame(df).xs('최근 연간 실적', axis=1)
        quater_date = pd.DataFrame(df).xs('최근 분기 실적', axis=1)
        print(annual_date)


if __name__ == '__main__':
    dbu = CompanyInfo()
    dbu.get_finstate_naver('005930')

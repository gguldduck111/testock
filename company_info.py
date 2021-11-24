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
        URL = f"https://finance.naver.com/item/main.nhn?code={code}"
        r = requests.get(URL)
        df = pd.read_html(r.text)[3]
        df.set_index(df.columns[0], inplace=True)
        df.index.rename('주요재무정보', inplace=True)
        df.columns = df.columns.droplevel(2)
        annual_date = pd.DataFrame(df).xs('최근 연간 실적', axis=1)
        quater_date = pd.DataFrame(df).xs('최근 분기 실적', axis=1)
        quater_date.rename(index={
            '주요재무정보': 'main',
            '매출액': 'sales',
            '영업이익': 'operatingProfit',
            '당기순이익': 'netProfit',
            '영업이익률': 'operatingProfitRate',
            '순이익률': 'netProfitRate',
            'ROE(지배주주)': 'roe',
            '부채비율': 'deptRate',
            '당좌비율': 'checkingRate',
            '유보율': 'reserveRate',
            'EPS(원)': 'eps',
            'PER(배)': 'per',
            'BPS(원)': 'bps',
            'PBR(배)': 'pbr',
            '주당배당금(원)': 'dividend',
            '시가배당률(%)': 'marketDividendRate',
            '배당성향(%)': 'dividendPropensity',
        }, inplace=True)
        annual_date.rename(index={
            '주요재무정보': 'main',
            '매출액': 'sales',
            '영업이익': 'operatingProfit',
            '당기순이익': 'netProfit',
            '영업이익률': 'operatingProfitRate',
            '순이익률': 'netProfitRate',
            'ROE(지배주주)': 'roe',
            '부채비율': 'deptRate',
            '당좌비율': 'checkingRate',
            '유보율': 'reserveRate',
            'EPS(원)': 'eps',
            'PER(배)': 'per',
            'BPS(원)': 'bps',
            'PBR(배)': 'pbr',
            '주당배당금(원)': 'dividend',
            '시가배당률(%)': 'marketDividendRate',
            '배당성향(%)': 'dividendPropensity',
        }, inplace=True)
        print(annual_date)


if __name__ == '__main__':
    dbu = CompanyInfo()
    dbu.get_finstate_naver('005930')

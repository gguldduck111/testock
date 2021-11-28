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
        self.conn = pymysql.connect(host='localhost', user='root',
                                    password='djajSK1@90#', db='stock', charset='utf8')

        with self.conn.cursor() as curs:
            asql = """
            CREATE TABLE IF NOT EXISTS company_annual_finance_info (
                code VARCHAR(20),
                date VARCHAR(10),
                sales BIGINT(20),
                operatingProfit BIGINT(20),
                operatingProfitRate FLOAT,
                netProfitRate FLOAT,
                roe FLOAT,
                deptRate FLOAT,
                checkingRate FLOAT,
                reserveRate FLOAT,
                eps BIGINT(20),
                per FLOAT,
                bps BIGINT(20),
                pbr FLOAT,
                dividend BIGINT(20),
                marketDividendRate FLOAT,
                dividendPropensity FLOAT,
                PRIMARY KEY (code, date)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            curs.execute(asql)

            qsql = """
            CREATE TABLE IF NOT EXISTS company_quarter_finance_info (
                code VARCHAR(20),
                date VARCHAR(10),
                sales BIGINT(20),
                operatingProfit BIGINT(20),
                operatingProfitRate FLOAT,
                netProfitRate FLOAT,
                roe FLOAT,
                deptRate FLOAT,
                checkingRate FLOAT,
                reserveRate FLOAT,
                eps BIGINT(20),
                per FLOAT,
                bps BIGINT(20),
                pbr FLOAT,
                dividend BIGINT(20),
                marketDividendRate FLOAT,
                dividendPropensity FLOAT,
                PRIMARY KEY (code, date)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            curs.execute(qsql)

        self.conn.commit()
        self.codes = dict()
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        self.codes = df['code']

    def get_finstate_naver(self, code):
        URL = f"https://finance.naver.com/item/main.nhn?code={code}"
        r = requests.get(URL)
        df = pd.read_html(r.text)[3]
        df.set_index(df.columns[0], inplace=True)
        df.index.rename('주요재무정보', inplace=True)
        df.columns = df.columns.droplevel(2)
        df = df.fillna('0')
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

        with self.conn.cursor() as curs:
            for i, r in annual_date.iteritems():
                sql = f"REPLACE INTO company_annual_finance_info VALUES ('{code}', " \
                    f"'{i}', {r.sales}, {r.operatingProfit}, {r.netProfit}, {r.operatingProfitRate}, " \
                    f"{r.netProfitRate}, {r.roe}, {r.deptRate}, {r.checkingRate}, " \
                    f"{r.reserveRate}, {r.eps}, {r.per}, {r.bps}, "\
                    f"{r.dividend}, {r.marketDividendRate}, {r.dividendPropensity})"

                print(sql)
                curs.execute(sql)

            for i, r in quater_date.iteritems():
                sql = f"REPLACE INTO company_quarter_finance_info VALUES ('{code}', " \
                    f"'{i}', {r.sales}, {r.operatingProfit}, {r.netProfit}, {r.operatingProfitRate}, " \
                    f"{r.netProfitRate}, {r.roe}, {r.deptRate}, {r.checkingRate}, " \
                    f"{r.reserveRate}, {r.eps}, {r.per}, {r.bps}, " \
                    f"{r.dividend}, {r.marketDividendRate}, {r.dividendPropensity})"

                curs.execute(sql)
            self.conn.commit()

    def execute_finance_info(self):
        for idx in range(len(self.codes)):
            self.get_finstate_naver(self.codes.values[idx])


if __name__ == '__main__':
    dbu = CompanyInfo()
    dbu.execute_finance_info()
    print('complete')

import pandas as pd
from bs4 import BeautifulSoup
import urllib, pymysql, calendar, time, json
import urllib.request as urllib
from datetime import datetime
from threading import Timer
import requests
import numpy as np


class DBUpdater:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root',
                                    password='djajSK1@90#', db='stock', charset='utf8')

        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """

            print(sql)
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def read_krx_code(self):
        """KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트 한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last" \
                          f"_update) VALUES ('{code}', '{company}', '{today}')"
                    print(sql)
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx + 1:04d} REPLACE INTO company_info " \
                          f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print('')

    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            opener = urllib.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]

            siseUrl = f'https://finance.naver.com/item/sise_day.nhn?code={code}'
            ifdf = self.read_institution_foreigner(code)
            req = requests.get(siseUrl, headers={'User-agent': 'Mozilla/5.0'})
            html = BeautifulSoup(req.text, 'lxml')
            pgRR = html.find('td', class_='pgRR')
            s = str(pgRR.a['href']).split('=')
            lastPage = s[-1]

            df = pd.DataFrame()
            pages = min(int(lastPage), pages_to_fetch)
            print(pages);exit;
            for page in range(1, pages + 1):
                pageUrl = '{}&page={}'.format(siseUrl, page)
                response = opener.open(pageUrl)
                df = df.append(pd.read_html(response.read())[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
                      format(tmnow, company, code, page, pages), end="\r")

            df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff'
                , '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
            df['date'] = df['date'].replace('.', '-')
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[
                ['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]

            if ifdf.empty:
                df['institution'] = 0
                df['foreigner'] = 0
            else:
                df['institution'] = ifdf[['institution']]
                df['foreigner'] = ifdf[['foreigner']]
            df = df.fillna(0)
        except Exception as e:
            print('Exception occured :', str(e))
            return None
        return df

    def replace_into_db(self, df, num, code, company):
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', " \
                      f"'{r.date}', {r.open}, {r.high}, {r.low}, {r.close}, " \
                      f"{r.diff}, {r.volume}, {r.institution}, {r.foreigner})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_' \
                  'price [OK]'.format(datetime.now().strftime('%Y-%m-%d' \
                                                              ' %H:%M'), num + 1, company, code, len(df)))

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])

    def update_golden_cross(self):
        sql = "SELECT date FROM daily_price ORDER BY date DESC LIMIT 0,1"
        dfdate = pd.read_sql(sql, self.conn)
        date = dfdate['date'].values[0]

        opener = urllib.build_opener()

        opener.addheaders = [('User-agent', 'Mozilla/5.0')]

        pageUrl = 'https://finance.naver.com/sise/item_gold.nhn'

        req = requests.get(pageUrl, headers={'User-agent': 'Mozilla/5.0'})
        html = BeautifulSoup(req.text, 'lxml')
        table = html.find('table', class_='type_5')
        response = opener.open(pageUrl)
        df = pd.DataFrame()
        df = df.append(pd.read_html(response, encoding='euc-kr')[1])
        df = df.fillna(0)
        df = df.rename(columns={'종목명': 'name', '현재가': 'close', '전일비': 'different', '거래량': 'volume'})
        df[['close', 'different', 'volume']] = df[['close', 'different', 'volume']].astype(np.int64)
        df['date'] = date
        df.head(5)
        df = df[['date', 'name', 'close', 'different', 'volume']]
        curs = self.conn.cursor()

        for idx in range(len(df)):
            if df.name[idx] != 0:
                codeSql = f'SELECT code FROM company_info WHERE company = "{df.name[idx]}"'
                codeDf = pd.read_sql(codeSql, self.conn)
                if not codeDf.empty:
                    code = codeDf['code'].values[0]
                    update_date = datetime.now().strftime('%Y-%m-%d')
                    sql = f"REPLACE INTO golden_cross (code, date, close, diff, volume, last_update) " \
                        f"VALUES ('{code}', '{df.date[idx]}', '{df.close[idx]}', '{df.different[idx]}', '{df.volume[idx]}', '{update_date}')"
                    curs.execute(sql)
        self.conn.commit()
        print("insert complete golden cross")

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트"""
        self.update_comp_info()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)

        self.update_daily_price(pages_to_fetch)
        # self.update_golden_cross()
        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1,
                                   hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17,
                                   minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0,
                                   second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds
        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ... ".format(tmnext.strftime
                                                         ('%Y-%m-%d %H:%M')))
        t.start()
    def read_institution_foreigner(self,code):
        opener = urllib.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        pageUrl = f'https://finance.naver.com/item/frgn.nhn?code={code}'
        response = opener.open(pageUrl)
        df = pd.DataFrame()
        df = df.append(pd.read_html(response, encoding='euc-kr')[2])
        df = df.fillna(0)
        df = df.rename(columns={'날짜': 'date', '기관': 'institution', '외국인': 'foreigner', '순매매량': 'buy'})
        df = df[['date', 'institution', 'foreigner']]
        return_df = pd.DataFrame(columns=['date', 'institution', 'foreigner'])
        for r in df.itertuples():
            if r[1] != 0:
                date = r[1]
                institution_tmp = r[2].replace('+', '') if type(r[2]) != float else int(r[2])
                foreigner_tmp = r[3].replace('+', '') if type(r[3]) != float else int(r[3])
                institution = np.int64(institution_tmp.replace(',', '')) if type(institution_tmp) != int else institution_tmp
                foreigner = np.int64(foreigner_tmp.replace(',', '')) if type(foreigner_tmp) != int else foreigner_tmp
                tmp_list = [date, institution, foreigner]
                a_series = pd.Series(tmp_list, index=return_df.columns)
                return_df = return_df.append(a_series, ignore_index=True)
        return_df = return_df.shift()[1:]
        return return_df

if __name__ == '__main__':
    dbu = DBUpdater()
    # dbu.update_comp_info()
    dbu.execute_daily()
    # dbu.update_golden_cross()

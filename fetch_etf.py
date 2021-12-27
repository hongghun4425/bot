VER = "0.1.0"
print(f'fetch_etf Version: {VER}')

import pymysql
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL

from library import cf
from library.open_api import escape_percentage

pymysql.install_as_MySQLdb()

dbl_url = URL(
    drivername='mysql+mysqldb',
    username=cf.db_id,
    password=cf.db_passwd,
    host=cf.db_ip,
    port=cf.db_port,
    database='daily_buy_list'
)

dbl_engine = create_engine(dbl_url, encoding='utf-8')
event.listen(dbl_engine, 'before_execute', escape_percentage, retval=True)
dates = dbl_engine.execute("""
    SELECT table_name
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = "daily_buy_list" AND table_name REGEXP "^[0-9]+"
""")
etf_code_names = dbl_engine.execute(
    'SELECT code_name FROM stock_etf'
).fetchall()

daily_craw_list = dbl_engine.execute(
    'SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "daily_craw"'
).fetchall()

for etf_name, in etf_code_names:
    if etf_name.lower() not in map(lambda x: x[0].lower(), daily_craw_list):
        print('콜렉터를 통해 ETF 종목의 daily_craw를 모두 받으신 후 사용해주시기 바랍니다.')
        exit(1)

for d, in dates:
    print(f'{d}에 ETF 종목들을 넣는 중...')

    for t_name, in etf_code_names:
        check_query = f"""
            SELECT 1 FROM `{d}` WHERE code_name = '{t_name}'
        """
        rows = dbl_engine.execute(check_query)
        if not rows.fetchall():
            insert_sql = f"""
                insert into `{d}` (`index`, date, check_item, code, code_name, d1_diff_rate, close, open, high, low, volume,
                            clo5, clo10, clo20, clo40, clo60, clo80, clo100, clo120, clo5_diff_rate, clo10_diff_rate,
                            clo20_diff_rate, clo40_diff_rate, clo60_diff_rate, clo80_diff_rate, clo100_diff_rate,
                            clo120_diff_rate, yes_clo5, yes_clo10, yes_clo20, yes_clo40, yes_clo60, yes_clo80, yes_clo100,
                            yes_clo120, vol5, vol10, vol20, vol40, vol60, vol80, vol100, vol120)
                    select * from daily_craw.`{t_name}` where date = '{d}' AND NOT EXISTS (SELECT b.code FROM `{d}` b);
            """
            dbl_engine.execute(insert_sql)

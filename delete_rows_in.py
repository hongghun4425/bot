import sys

import pymysql
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import InternalError

from library import cf
from library.logging_pack import logger
from library.open_api import escape_percentage

pymysql.install_as_MySQLdb()

print("입력 하신 날짜를 포함한 그 이후의 데이터는 모두 지워집니다.")
target_date = input('날짜를 입력해 주세요. (ex 20150203): ')
if not (target_date.isnumeric() and len(target_date) == 8):
    print('정확한 날짜를 입력해주세요.')
    sys.exit(1)


db_url = URL(
    drivername='mysql+mysqldb',
    username=cf.db_id,
    password=cf.db_passwd,
    host=cf.db_ip,
    port=cf.db_port
)

engine = create_engine(db_url, encoding='utf-8')
event.listen(engine, 'before_execute', escape_percentage, retval=True)

table_names = engine.execute(
    'SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "daily_craw"'
)
for name, in table_names:
    logger.debug(f"daily_craw.{name}에서 삭제 중..")
    name = name.replace('%', '%%')
    engine.execute(f"DELETE FROM daily_craw.`{name}` WHERE date >= '{target_date}'")  # daily_craw의 테이블에서 해당 날짜 보다 최신인 행들을 삭제

try:
    dbl_tables = engine.execute(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'daily_buy_list'"
    )
    for table_name, in dbl_tables:  # daily_buy_list에서 해당 날짜부터 테이블 삭제
        if table_name.isnumeric() and table_name >= target_date:
            logger.debug(f"daily_buy_list.{table_name}에서 삭제 중..")
            engine.execute(f"DROP TABLE daily_buy_list.`{table_name}`")
except InternalError:
    pass

print("성공적으로 삭제하였습니다.")

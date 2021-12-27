
import pandas as pd
from sqlalchemy import create_engine
from library import cf
import talib.abstract as ta
import pymysql.cursors
import numpy as np
from library.logging_pack import *

logger.debug("subindex시작!!!!")

pymysql.install_as_MySQLdb()

daily_craw_engine=create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_craw",
            encoding='utf-8')
daily_buy_list_engine = create_engine(
    "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/daily_buy_list" ,
    encoding='utf-8')
simul_engine=create_engine(
            "mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/simulator11",
            encoding='utf-8')
min_craw_engine = create_engine("mysql+mysqldb://" + cf.db_id + ":" + cf.db_passwd + "@" + cf.db_ip + ":" + cf.db_port + "/min_craw",
            encoding='utf-8')

stand_date = '20070903'

#데이터 변환
class subindex:
    def __init__(self):
        logger.debug("subindex 함수로 들어왔다!!")

    def collecting(self):
        co_sql = f"select TABLE_NAME FROM information_schema.tables WHERE table_schema = 'daily_craw'"


        target_code = daily_craw_engine.execute(co_sql).fetchall()
        num = len(target_code)

        for i in range(num):
            self.db_name = target_code[i][0]
            self.db_name = self.db_name.replace("%", "%%")
            self.collect_db()
            print(self.db_name , "을 가져온다!")

    def collect_db(self):
        # 데이터 불러오기
        sql = "select date,code,code_name,close,low,high,volume from daily_craw.`%s` where Date >= %s order by Date "
        rows = daily_craw_engine.execute(sql%(self.db_name,stand_date)).fetchall()
        three_s = pd.DataFrame(rows, columns=['date', 'code', 'code_name', 'close', 'low', 'high', 'volume'])
        three_s = three_s.fillna(0)

        # 데이터 변환
        th_date = list(np.asarray(three_s['date'].tolist()))
        th_date_np = np.array(th_date, dtype='f8')
        th_close = list(np.asarray(three_s['close'].tolist()))
        th_close_np = np.array(th_close, dtype='f8')
        th_high = list(np.asarray(three_s['high'].tolist()))
        th_high_np = np.array(th_high, dtype='f8')
        th_low = list(np.asarray(three_s['low'].tolist()))
        th_low_np = np.array(th_low, dtype='f8')
        th_volume = list(np.asarray(three_s['volume'].tolist()))
        th_volume_np = np.array(th_volume, dtype='f8')

        # 보조지표 계산
        th_cci = ta._ta_lib.CCI(th_high_np, th_low_np, th_close_np, 9)
        th_rsi = ta._ta_lib.RSI(th_close_np, 14)
        th_OBV = ta._ta_lib.OBV(th_close_np, th_volume_np)
        th_macd, th_macd_signal, th_macd_hist = ta._ta_lib.MACD(th_close_np, fastperiod=12, slowperiod=26,
                                                                signalperiod=9)
        th_stoch_slowk, th_stoch_slowd = ta._ta_lib.STOCH(th_high_np, th_low_np, th_close_np,
                                                          fastk_period=5, slowk_period=3, slowk_matype=0,
                                                          slowd_period=3, slowd_matype=0)
        th_BBAND_U, th_BBAND_M, th_BBAND_L = ta._ta_lib.BBANDS(th_close_np, timeperiod=5, nbdevup=2, nbdevdn=2,
                                                               matype=0)

        # nan을 모두 0으로 전환
        np.nan_to_num(th_cci, copy=False)
        np.nan_to_num(th_rsi, copy=False)
        np.nan_to_num(th_macd, copy=False)
        np.nan_to_num(th_macd_signal, copy=False)
        np.nan_to_num(th_macd_hist, copy=False)
        np.nan_to_num(th_stoch_slowk, copy=False)
        np.nan_to_num(th_stoch_slowd, copy=False)
        np.nan_to_num(th_BBAND_L, copy=False)
        np.nan_to_num(th_BBAND_M, copy=False)
        np.nan_to_num(th_BBAND_U, copy=False)
        np.nan_to_num(th_OBV, copy=False)

        # DataFrame 화 하기
        df_cci = pd.DataFrame(th_cci, columns=['cci'])
        df_rsi = pd.DataFrame(th_rsi, columns=['rsi'])
        df_macd = pd.DataFrame(th_macd, columns=['macd'])
        df_macd_signal = pd.DataFrame(th_macd_signal, columns=['macd_signal'])
        df_macd_hist = pd.DataFrame(th_macd_hist, columns=['macd_hist'])
        df_stoch_slowk = pd.DataFrame(th_stoch_slowk, columns=['stoch_slowk'])
        df_stoch_slowd = pd.DataFrame(th_stoch_slowd, columns=['stoch_slowd'])
        df_BBand_U = pd.DataFrame(th_BBAND_U, columns=['BBand_U'])
        df_BBand_M = pd.DataFrame(th_BBAND_M, columns=['BBand_M'])
        df_BBand_L = pd.DataFrame(th_BBAND_L, columns=['BBand_L'])
        df_OBV = pd.DataFrame(th_OBV, columns=['OBV'])

        # 모든 보조지표 합치기
        subindex = pd.concat(
            [three_s, df_cci, df_rsi, df_OBV, df_macd, df_macd_signal, df_macd_hist, df_BBand_U, df_BBand_M,
             df_BBand_L /
             df_stoch_slowk, df_stoch_slowd], axis=1)

        # mysql 에 테이블 생성
        subindex.to_sql(name='subindex', con=daily_buy_list_engine, if_exists='append')


#모든 종목 데이터 한바퀴씩
subindex=subindex()
subindex.collecting()

logger.debug("모든 subindex 수집끝!!!!")

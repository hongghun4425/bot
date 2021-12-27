
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
        sql = "select date,code,vol10,code_name,open,close,low,high,volume from daily_craw.`%s` where Date >= %s order by Date "
        rows = daily_craw_engine.execute(sql%(self.db_name,stand_date)).fetchall()
        three_s = pd.DataFrame(rows, columns=['date', 'code','vol10' ,'code_name','open' ,'close', 'low', 'high', 'volume'])
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
        th_open = list(np.asarray(three_s['open'].tolist()))
        th_open_np = np.array(th_open, dtype='f8')
        th_vol10 = list(np.asarray(three_s['vol10'].tolist()))
        th_vol10_np = np.array(th_vol10, dtype='f8')



        #주가고가저가 변동폭
        th_diff =((three_s['high']-three_s['low'])/three_s['high'])*100
        # 30일간 주가최저최고 변동폭 클때
        th_diff30 = th_diff.rolling(30).max()

        # 보조지표 계산
        th_cci = ta._ta_lib.CCI(th_high_np, th_low_np, th_close_np, 9)
        th_cci60 = ta._ta_lib.CCI(th_high_np, th_low_np, th_close_np, 60)
        ##rsi
        th_rsi = ta._ta_lib.RSI(th_close_np, 14)
        th_rsi5 = ta._ta_lib.RSI(th_close_np, 5)


        th_OBV = ta._ta_lib.OBV(th_close_np, th_volume_np)
        th_macd, th_macd_signal, th_macd_hist = ta._ta_lib.MACD(th_close_np, fastperiod=12, slowperiod=26,
                                                                signalperiod=9)
        th_stoch_slowk, th_stoch_slowd = ta._ta_lib.STOCH(th_high_np, th_low_np, th_close_np,
                                                          fastk_period=10, slowk_period=2, slowk_matype=0,
                                                          slowd_period=2, slowd_matype=0)

        ##책에따라 12일선 기준으로 바꿈
        th_BBAND_U, th_BBAND_M, th_BBAND_L = ta._ta_lib.BBANDS(th_close_np, timeperiod=20, nbdevup=2, nbdevdn=2,
                                                               matype=0)
        th_BBAND_U14, th_BBAND_M14, th_BBAND_L14 = ta._ta_lib.BBANDS(th_close_np, timeperiod=14, nbdevup=2, nbdevdn=2,
                                                               matype=0)
        th_BBAND_WIDE = (th_BBAND_U-th_BBAND_L)/th_BBAND_M
        th_BBAND_WIDE14 = (th_BBAND_U14 - th_BBAND_L14) / th_BBAND_M14
        th_pb=(th_close_np-th_BBAND_L) / (th_BBAND_U-th_BBAND_L)
        th_pb14 = (th_close_np - th_BBAND_L14) / (th_BBAND_U14 - th_BBAND_L14)
        th_sar = ta._ta_lib.SAR(th_high_np, th_low_np,0.04,0.4)
        th_ibs = (th_close_np -th_low_np)/(th_high_np-th_low_np)
        th_dema5 = ta._ta_lib.DEMA(th_close_np, 5)
        th_dema20 = ta._ta_lib.DEMA(th_close_np,20)
        th_dema60 = ta._ta_lib.DEMA(th_close_np, 60)
        th_tema5 = ta._ta_lib.TEMA(th_close_np,5)
        th_tema20 = ta._ta_lib.TEMA(th_close_np, 20)
        th_tema60 = ta._ta_lib.TEMA(th_close_np, 60)
        #ema = 지수이동평균
        th_ema5 = ta._ta_lib.EMA(th_close_np, 5)
        th_ema20 = ta._ta_lib.EMA(th_close_np, 20)
        th_ema60 = ta._ta_lib.EMA(th_close_np, 60)
        th_ema112 = ta._ta_lib.EMA(th_close_np, 112)
        th_ema224 = ta._ta_lib.EMA(th_close_np, 224)
        th_ema448 = ta._ta_lib.EMA(th_close_np, 448)
        th_ema448diff = ((th_close_np-th_ema448)/th_close_np * 100)
        th_ema224diff = ((th_close_np-th_ema224)/th_close_np*100)
        th_ema112diff = ((th_close_np-th_ema112)/th_close_np*100)

        #ma 이동평균
        th_ma112  = ta._ta_lib.MA(th_close_np, 112)
        th_ma224  = ta._ta_lib.MA(th_close_np, 224)
        th_ma448 = ta._ta_lib.MA(th_close_np, 448)
        th_clo5diff = ((th_close_np - ta._ta_lib.MA(th_close_np, 5)) / th_close_np * 100)
        th_clo20diff = ((th_close_np - ta._ta_lib.MA(th_close_np, 20)) / th_close_np * 100)
        #dmi값들 14->11로 고쳐씀
        th_pdi = ta._ta_lib.PLUS_DI(th_high_np,th_low_np,th_close_np, 11)
        th_mdi = ta._ta_lib.MINUS_DI(th_high_np, th_low_np, th_close_np, 11)
        th_dm = ta._ta_lib.PLUS_DM(th_high_np,th_low_np, 11)
        th_adx = ta._ta_lib.ADX(th_high_np,th_low_np,th_close_np, 14)
        th_adxr = ta._ta_lib.ADXR(th_high_np, th_low_np, th_close_np, 14)
        th_obvsig9 =ta._ta_lib.MA(ta._ta_lib.OBV(th_close_np, th_volume_np),9)



        #윌리엄 변동율
        th_williumr = ta._ta_lib.WILLR(th_high_np,th_low_np,th_close_np, 14)
        th_mfi = ta._ta_lib.MFI(th_high_np,th_low_np,th_close_np,th_volume_np, 14)
        #거래량 오실레이터공식 10일
        th_ad = ((th_close_np-th_open_np)/(th_high_np-th_low_np) * th_volume_np / th_vol10_np*10)
#       #일중강도
        th_ll = (2*th_close_np-th_high_np-th_low_np)/(th_high_np-th_low_np) * th_volume_np
        # nan을 모두 0으로 전환
        np.nan_to_num(th_cci, copy=False)
        np.nan_to_num(th_cci60, copy=False)
        np.nan_to_num(th_rsi, copy=False)
        np.nan_to_num(th_macd, copy=False)
        np.nan_to_num(th_macd_signal, copy=False)
        np.nan_to_num(th_macd_hist, copy=False)
        np.nan_to_num(th_stoch_slowk, copy=False)
        np.nan_to_num(th_stoch_slowd, copy=False)
        np.nan_to_num(th_BBAND_L, copy=False)
        np.nan_to_num(th_BBAND_M, copy=False)
        np.nan_to_num(th_BBAND_U, copy=False)
        np.nan_to_num(th_BBAND_L14, copy=False)
        np.nan_to_num(th_BBAND_M14, copy=False)
        np.nan_to_num(th_BBAND_U14, copy=False)
        np.nan_to_num(th_OBV, copy=False)
        np.nan_to_num(th_sar, copy=False)
        np.nan_to_num(th_dema5, copy=False)
        np.nan_to_num(th_dema20, copy=False)
        np.nan_to_num(th_dema60, copy=False)
        np.nan_to_num(th_tema5, copy=False)
        np.nan_to_num(th_tema20, copy=False)
        np.nan_to_num(th_tema60, copy=False)
        np.nan_to_num(th_ema5, copy=False)
        np.nan_to_num(th_ema112diff, copy=False)
        np.nan_to_num(th_ema224diff, copy=False)
        np.nan_to_num(th_ema448diff, copy=False)
        np.nan_to_num(th_ema20, copy=False)
        np.nan_to_num(th_ema60, copy=False)
        np.nan_to_num(th_ema112, copy=False)
        np.nan_to_num(th_ema224, copy=False)
        np.nan_to_num(th_ema448, copy=False)
        np.nan_to_num(th_ma112, copy=False)
        np.nan_to_num(th_ma224, copy=False)
        np.nan_to_num(th_ma448, copy=False)
        np.nan_to_num(th_pdi, copy=False)
        np.nan_to_num(th_mdi, copy=False)
        np.nan_to_num(th_dm, copy=False)
        np.nan_to_num(th_adx, copy=False)
        np.nan_to_num(th_adxr, copy=False)
        np.nan_to_num(th_williumr, copy=False)
        np.nan_to_num(th_pb, copy=False)
        np.nan_to_num(th_pb14, copy=False)
        np.nan_to_num(th_BBAND_WIDE, copy=False)
        np.nan_to_num(th_BBAND_WIDE14, copy=False)
        np.nan_to_num(th_mfi, copy=False)
        np.nan_to_num(th_ll, copy=False)
        np.nan_to_num(th_ad, copy=False)
        np.nan_to_num(th_rsi5, copy=False)
        np.nan_to_num(th_ibs, copy=False)
        np.nan_to_num(th_diff, copy=False)
        np.nan_to_num(th_diff30, copy=False)
        np.nan_to_num(th_obvsig9, copy=False)




        # DataFrame 화 하기
        df_ad = pd.DataFrame(th_ad, columns=['ad'])
        df_cci = pd.DataFrame(th_cci, columns=['cci'])
        df_cci60 = pd.DataFrame(th_cci, columns=['cci60'])
        df_rsi5 = pd.DataFrame(th_rsi5, columns=['rsi5'])


        df_rsi = pd.DataFrame(th_rsi, columns=['rsi'])

        df_macd = pd.DataFrame(th_macd, columns=['macd'])
        df_macd_signal = pd.DataFrame(th_macd_signal, columns=['macd_signal'])
        df_macd_hist = pd.DataFrame(th_macd_hist, columns=['macd_hist'])
        df_stoch_slowk = pd.DataFrame(th_stoch_slowk, columns=['stoch_slowk'])
        df_stoch_slowd = pd.DataFrame(th_stoch_slowd, columns=['stoch_slowd'])
        #볼린저밴드
        df_BBand_U = pd.DataFrame(th_BBAND_U, columns=['BBand_U'])
        df_BBand_M = pd.DataFrame(th_BBAND_M, columns=['BBand_M'])
        df_BBand_L = pd.DataFrame(th_BBAND_L, columns=['BBand_L'])
        df_BBand_U14 = pd.DataFrame(th_BBAND_U, columns=['BBand_U14'])
        df_BBand_M14 = pd.DataFrame(th_BBAND_M, columns=['BBand_M14'])
        df_BBand_L14 = pd.DataFrame(th_BBAND_L, columns=['BBand_L14'])
        df_ibs = pd.DataFrame(th_ibs, columns=['ibs'])
        df_pb14 = pd.DataFrame(th_pb, columns=['pb14'])
        df_obvsig9 = pd.DataFrame(th_obvsig9, columns=['obvsig9'])
        df_OBV = pd.DataFrame(th_OBV, columns=['OBV'])
        df_sar = pd.DataFrame(th_sar, columns=['sar'])
        # 2중종합지수
        df_dema5 = pd.DataFrame(th_dema5, columns=['dema5'])

        df_dema20 = pd.DataFrame(th_dema20, columns=['dema20'])
        df_dema60 = pd.DataFrame(th_dema60, columns=['dema60'])
        #3중종합지수
        df_tema5 = pd.DataFrame(th_tema5, columns=['tema5'])
        df_tema20 = pd.DataFrame(th_tema20, columns=['tema20'])
        df_tema60 = pd.DataFrame(th_tema60, columns=['tema60'])
        # 평균지수
        df_ema5 = pd.DataFrame(th_ema5, columns=['ema5'])
        df_ema112diff = pd.DataFrame(abs(th_ema112diff), columns=['ema112diff'])
        df_ema224diff = pd.DataFrame(abs(th_ema224diff), columns=['ema224diff'])
        df_ema448diff = pd.DataFrame(abs(th_ema448diff), columns=['ema448diff'])
        df_ema20 = pd.DataFrame(th_ema20, columns=['ema20'])
        df_ema60 = pd.DataFrame(th_ema60, columns=['ema60'])
        df_ema112 = pd.DataFrame(th_ema112, columns=['ema112'])
        df_ema224 = pd.DataFrame(th_ema224, columns=['ema224'])
        df_ema448 = pd.DataFrame(th_ema224, columns=['ema448'])
        df_ma112 = pd.DataFrame(th_ma112, columns=['ma112'])
        df_ma224 = pd.DataFrame(th_ma224, columns=['ma224'])
        df_ma448 = pd.DataFrame(th_ma448, columns=['ma448'])
        df_pdi = pd.DataFrame(th_pdi, columns=['pdi'])
        df_mdi = pd.DataFrame(th_mdi, columns=['mdi'])
        df_dm = pd.DataFrame(th_dm, columns=['dm'])
        df_adx = pd.DataFrame(th_adx, columns=['adx'])
        df_adxr = pd.DataFrame(th_adxr, columns=['adxr'])
        df_williumr = pd.DataFrame(th_williumr, columns=['williumr'])
        df_pb = pd.DataFrame(th_pb, columns=['pb'])
        df_th_BBAND_WIDE = pd.DataFrame(th_BBAND_WIDE, columns=['th_BBAND_WIDE'])
        df_th_BBAND_WIDE14 = pd.DataFrame(th_BBAND_WIDE14, columns=['th_BBAND_WIDE14'])
        df_mfi = pd.DataFrame(th_mfi, columns=['mfi'])
        df_ll = pd.DataFrame(th_ll, columns=['ll'])
        df_diff = pd.DataFrame(abs(th_diff), columns=['diff'])
        df_diff30 = pd.DataFrame(abs(th_diff30), columns=['diff30'])
        df_clo5diff = pd.DataFrame(abs(th_clo5diff), columns=['clo5diff'])
        df_clo20diff = pd.DataFrame(abs(th_clo20diff), columns=['clo20diff'])





        #일목균형표 구름대 형성
        # Calculate Tenkan-sen
        #20일이내 RSI 저점계산

        


        #9일이내 최고,최저점 계산
        high_9 = three_s['high'].rolling(9).max()
        low_9 = three_s['low'].rolling(9).min()
        tsl = (high_9 + low_9) / 2

        # Calculate Kijun-sen
        high_26 = three_s['high'].rolling(26).max()
        low_26 = three_s['low'].rolling(26).min()
        ksl = (high_26 + low_26) / 2
        df_ksl=pd.DataFrame(ksl, columns=['ksl'])
        ksldiff = ((three_s['close']-ksl)/ksl*100)
        df_ksldiff = pd.DataFrame(abs(ksldiff), columns=['ksldiff'])

        # Calculate Senkou Span A
        ssp1 = ((tsl + ksl) / 2).shift(26)
        df_ssp1 = pd.DataFrame(ssp1, columns=['ssp1'])
        # Calculate Senkou Span B
        high_52 = three_s['high'].rolling(52).max()
        low_52 = three_s['low'].rolling(52).min()
        ssp2 = ((high_52 + low_52) / 2).shift(26)
        ssp2diff = ((three_s['close'] - ssp2) / ssp2 * 100)
        df_ssp2 = pd.DataFrame(ssp2, columns=['ssp2'])
        df_ssp2diff = pd.DataFrame(abs(ssp2diff), columns=['ssp2diff'])



        # 모든 보조지표 합치기 일목균형표는 선행스팬1,2만추가함 쿼리문이용시 구름떼 위 = 선행1,2위에 종가가있으면됨

        subindex = pd.concat(
            [three_s,df_ksldiff,df_diff,df_diff30,df_ssp2diff, df_cci, df_rsi, df_cci60 ,df_rsi5, df_OBV, df_macd, df_macd_signal, df_macd_hist, df_BBand_U, df_BBand_M, df_ibs,
             df_BBand_L ,df_th_BBAND_WIDE, df_mfi,df_ll, df_ad, df_BBand_U14, df_BBand_M14,df_BBand_L14, df_pb14,df_th_BBAND_WIDE14,
             df_stoch_slowk, df_stoch_slowd , df_sar , df_dema5 , df_dema20 , df_dema60 , df_tema5 , df_tema20 , df_tema60,df_obvsig9,
             df_ema5 ,df_ema112diff,df_ema224diff,df_ema448diff,df_ema20 ,df_ema60 ,df_ema112,df_ema224,df_ema448,df_ksl,df_ssp1 ,
             df_ssp2, df_pb ,df_ma112,df_ma224,df_ma448,df_pdi,df_mdi,df_dm,df_adx,df_adxr,df_williumr,df_clo5diff,df_clo20diff] ,axis=1)

        # mysql 에 테이블 생성
        subindex.to_sql(name='subindex', con=daily_buy_list_engine, if_exists='append')




#모든 종목 데이터 한바퀴씩
subindex=subindex()
subindex.collecting()

logger.debug("모든 subindex 수집끝!!!!")

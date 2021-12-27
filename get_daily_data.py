import sys
from PyQt5.QtWidgets import *
import openapi


class get_daily_data():
    def __init__(self):
        self.api = openapi.Openapi()
        self.run()

    def run(self):
        # get_total_data : 특정 종목의 1985년 이후 특정 날짜까지의 주가 데이터를 모두 가져오는 함수
        # 첫 번째 parameter : 종목코드 (ex.  005930 : 삼성전자 종목 코드) *종목별 코드 번호 존재
        # 두 번째 parameter : 데이터를 가져올 최종 날짜 설정

        data = self.api.get_total_data('005930', '20200424')

        # open : 시작가
        # high : 최고가
        # low : 최저가
        # close : 종가
        # volume : 거래량

        print(data)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    get_daily_data()

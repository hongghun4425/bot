from PyQt5.QtWidgets import *
from library.open_api import *


class OpenTest:
    def __init__(self):
        self.open_api = open_api()
        self.open_api.py_gubun = "collector"
        self.run()

    def run(self):
        # Part1. 기본적인 OpenAPI 메소드 활용 () -----------------------------------------------------------------------
        # koastudio -> 파일 -> OpenAPI 접속 -> 로그인(모의투자만 접속 가능)  ** 개발가이드 읽어보시면 도움이 됩니다. (조회 제한 정책 등)



        # *종목정보함수(koastudio 개발가이드/기타함수/종목정보관련함수 참고) : GetCodeListByMarket, GetMasterListedStockCnt ...
        # dynamicCall : OpenAPI 메소드를 호출 하기 위한 함수
        #               (PyQt5 패키지의 QAxContainer 모듈에 포함 - open_api.py 파일 상단의 from PyQt5.QAxContainer import *)
        # BSTR : 문자열로 인자 값을 보내라(Qstring)
        ex1 = self.open_api.dynamicCall('GetCodeListByMarket(QString)', '8')
        print(f"ex1 -- ETF 종목코드 리스트: {ex1}")

        ex2 = self.open_api.dynamicCall('GetMasterCodeName(QString)', '005930')
        print(f"ex2 -- 005930 종목명: {ex2}")

        ex3 = self.open_api.dynamicCall('GetMasterListedStockCnt(QString)', '005930')
        print(f"ex3 -- 삼성전자 상장 주식수: {ex3}")

        ex4 = self.open_api.dynamicCall('GetMasterConstruction(QString)', '005930')
        print(f"ex4 -- 삼성전자 감리 구분: {ex4}")

        ex5 = self.open_api.dynamicCall('GetMasterStockState(QString)', '005930')
        print(f"ex5 -- 삼성전자 증거금 비율, 거래정지여부, 관리종목여부 등(: {ex5}")

        ex14 = self.open_api.dynamicCall('GetConditionNameList()')





        # *테마관련함수(키움 OpenAPI+ 개발가이드 매뉴얼 참고 ** koastudio에는 가이드가 없음)
        # LONG : 정수형으로 인자 값을 보내라(int)
        ex6 = self.open_api.dynamicCall('GetThemeGroupList(int)', 1)
        print(f"ex6 -- 테마코드, 테마명 리스트: {ex6}")

        # 종목 코드에 알파벳이 붙어서 나오기 때문에 사용 시 가공이 필요함
        ex7 = self.open_api.dynamicCall('GetThemeGroupCode(QString)', '141')
        print(f"ex7 -- 141 테마코드(2차전지_소재)에 해당하는 종목코드: {ex7}")

        # get_theme_group_code 함수는 제공 되는 것이 아닌 사용자가 직접 만들어야함
        ex8 = self.open_api.get_theme_group_code('141')
        print(f"ex8 --  ex7 가공 버전: {ex8}")

        ex9 = self.open_api.get_theme_info()
        print(f"ex9 -- ex6, ex8 혼합 버전 (테마코드, 테마명 그리고 테마 그룹에 속하는 종목코드 리스트): {ex9}")





        # *특수함수(koastudio 개발가이드/기타함수/특수함수 참고)
        ex10 = self.open_api.KOA_Functions('GetMasterStockInfo', '005930')
        print(f"ex10 -- 주식종목 시장구분, 종목분류등 : {ex10}")
        # -----------------------------------------------------------------------------------------------------------







        # Part2. TR을 활용한 데이터 수집 -----------------------------------------------------
        # => CommRqData OpenAPI 메소드 활용
        # TR(Transaction) :  어떤 일을 하기위한 작업들의 모음. (서버에 데이터를 요청 -> 수신)
        print("ex11 -- 예수금 출력 ")
        # koastudio 좌측 하단 TR목록 / 'opw00001:예수금상세현황조회' 클릭 후 샘플 참고
        self.open_api.get_d2_deposit()

        print("ex12 -- 삼성전자 주가 데이터 ")
        # koastudio 좌측 하단 TR목록 / 'opt10081:주식일봉차트조회요청' 클릭 후 샘플 참고
        # get_total_data : 특정 종목의 1985년 이후 특정 날짜까지의 주가 데이터를 모두 가져오는 함수
        data = self.open_api.get_total_data('005930', '삼성전자', '20200424')
        print(data)

        print("ex13 -- 삼성전자 재정 데이터 ")
        # koastudio 좌측 하단 TR목록 / 'opt10001:주식기본정보요청' 클릭 후 샘플 참고
        data2 = self.open_api.get_stock_finance('005930')
        print(data2)
        # -----------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    app = QApplication(sys.argv)
    OpenTest()

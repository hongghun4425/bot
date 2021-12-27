# 이 코드가 완벽히 이해가 된다면 봇 개발 절반은 성공.
print("collector 프로그램이 시작 되었습니다!")
# crtl + alt + 좌우방향키 : 이전 커서 위치로

# 아래 세줄 -> mysql이라는 데이터베이스를 사용하기 위해 필요한 패키지들 -> 암기X! 그냥 다음에 mysql을 사용하고 싶으면 아래 세 줄을 복사, 붙여넣기

from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()



# -> openapi.py 라는 소스파일에 있는 Openapi 클래스만 가져와서 사용하고 싶은 경우. -> openapi에 import 된 라이브러리를 사용하고 싶지 않다!
# from openapi import Openapi
# openapi.py 라는 소스파일에 있는 모든 함수, 라이브러리, 클래스 등을 가져오고 싶을 경우 아래처럼!
from openapi import *

# get_item : 종목 리스트 가져오는 모듈
from get_item import StockItem


class Collector:
    def __init__(self):
        print("__init__ 함수에 들어왔습니다.")
        self.engine_bot = None
        # self.api 객체를 만든다. (Openapi클래스의 인스턴스)
        # Openapi() 클래스를 통해 self.api 객체를 만들면 openapi프로그램이 실행 되면서 증권사 계정과 연동이 된다.
        self.api = Openapi()
        # self.item 객체를 만든다. (StockItem클래스의 인스턴스)
        # 코스피, 코스닥 종목 리스트를 가져온다.
        self.item = StockItem()

    def db_setting(self, db_name, db_id, db_passwd, db_ip, db_port):
        print("db_setting 함수에 들어왔습니다.")
        # mysql 데이터베이스랑 연동하는 방식.
        self.engine_bot = create_engine("mysql+mysqldb://" + db_id + ":" + db_passwd + "@"
                                        + db_ip + ":" + db_port + "/" + db_name, encoding='utf-8')
    def print_stock_data(self):
        print("get_stock_data 함수에 들어왔습니다.")

        # self.item : StockItem 클래스의 인스턴스
        # self.item.code_df_kospi : self.item 객체의 속성
        print("코스피 종목 리스트 !!!")
        print(self.item.code_df_kospi)

        # self.item.code_df_kosdaq : self.item 객체의 속성
        print("코스닥 종목 리스트 !!!")
        print(self.item.code_df_kosdaq)

        # 아래 api의 함수들은 ctrl + 클릭해서 들어가보면 사용 방법 설명 되어 있음.
        # self.api : openapi클래스의 인스턴스
        # get_total_data : Openapi클래스의 메서드
        total_data = self.api.get_total_data('005930', '20200424')
        print("total_data: !!!")
        print(total_data)

        # get_one_day_option_data : Openapi클래스의 메서드
        one_data = self.api.get_one_day_option_data('005930', '20200424', 'close')
        print("one_data: !!!")
        print(one_data)



#  __name__ ? => 현재 모듈의 이름을 담고 있는 내장 변수, 우리는 colector라는 파일을 실행한다! -> 이 파일의 __name__ 내장변수에는 __main__ 이 저장되어있다
# import openapi를 통해서 openapi소스코드를 참고를 하는 경우 openapi 모듈의 __name__은 "openapi" 가 저장 되어 있다.
# openapi 파일을 실행하면 그때는 참고 한 것이 아니기 때문에 __name__에는 __main__ 이 저장 되어 있다.
print("collector_v2.py 의 __name__ 은?: ", __name__)
if __name__ == "__main__":
    print("__main__에 들어왔습니다.")
    # 아래는 키움증권 openapi를 사용하기 위해 사용하는 한 줄!
    app = QApplication(sys.argv)
    # c = collector() 이렇게 c라는 collector라는 클래스의 인스턴스를 만든다.
    # 아래 클래스를 호출하자마다 __init__ 함수가 실행이 된다.
    c = Collector()
    c.print_stock_data()

# 패키지 만들기 실습(package_test.py 파일 및 my_package 패키지 생성 후 실습)
# from 패키지 import 모듈 (ex. from my_package import sum)

from my_package import sum, sub
x = sum.sum_ab(1, 2)
print(x)

y = sub.sub_ab(3, 1)
print(y)

# from 패키지.모듈 import 함수, 클래스 (ex. from my_package.sum import sum_ab)
from my_package.sum import sum_ab, Calculator, print_name
from my_package.sub import sub_ab


# __name__ ? => 현재 모듈의 이름을 담고 있는 내장 변수
if __name__ == '__main__':
    x2 = sum_ab(3, 4)
    y2 = sub_ab(4, 1)
    print(f"x2 : {x2}, y2 : {y2}")

    c = Calculator(1, 2)
    print(f"c.sum() : {c.sum()}")


    print(f"package_test.py 의 __name__: {__name__}")
    print_name()
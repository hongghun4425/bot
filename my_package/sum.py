print("sum.py 파일 입니다!")

def sum_ab(x, y):
    return x + y

def print_name():
    print(f"sum.py 모듈의 __name__ : {__name__}")

class Calculator:
    def __init__(self, cx, cy):
        self.x = cx
        self.y = cy

    def sum(self):
        return self.x + self.y


if __name__ == '__main__':
    print("sum.py 파일의 if __name__ == '__main__': 구문 입니다!")
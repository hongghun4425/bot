####################### 1.조건검색 변수설정 ##################################
self.condition_scrNo = "1004"  # 조건검색 화면번호
self.condition_name = "조건검색식 이름"  # 조건검색할 조건검색식 이름을 여기에 넣는다.
self.condition_stock = {'code': [], 'code_name': [], 'check_item': [],
                        'check_daily_crawler': [], 'check_min_crawler': []}
###########################################################################


############ 2.조건검색식 이벤트 처리하는 메서드 ################################################
self.OnReceiveConditionVer.connect(self.condition_slot)  # 조건검색식 불러오는 이벤트
self.OnReceiveTrCondition.connect(self.condition_tr_slot)  # 조건검색한 종목리스트 불러오는 이벤트
############################################################################################


#################################### 3.조건검색 부분 #########################################################
    def get_condition(self):
        self.dynamicCall("GetConditionLoad()")  # 조건검색식 목록을 요청하는 함수
        self.condition_event_loop = QEventLoop()
        self.condition_event_loop.exec_()

    def condition_slot(self, lRet, sMsg):
        if lRet == 1:
            logger.debug("조건검색 호출처리: %s", sMsg)
            condition_list = self.dynamicCall("GetConditionNameList()")  # 조건검색식 목록 불러오는 함수
            condition_list = condition_list.split(';')[:-1]

            for l in condition_list:
                condition_list_index = l.split('^')[0]
                condition_name = l.split('^')[1]
                if condition_name == self.condition_name:
                    answer = self.dynamicCall("SendCondition(QString, QString, int, int)",
                                              self.condition_scrNo, condition_name, condition_list_index, 0)
                    if answer == 1:
                        logger.debug("%s 조건검색요청처리: 성공", condition_name)
                    else:
                        logger.debug("%s 조건검색요청처리: 실패", condition_name)
        else:
            logger.debug("조건검색 호출처리: ", sMsg)
            self.condition_event_loop.exit()

    def condition_tr_slot(self, sScrNo, strCodeList, strConditionName, nIndex, nNext):
        stock_code_list = strCodeList.split(';')[:-1]
        logger.debug("조건검색종목리스트: %s개", len(stock_code_list))
        for code in stock_code_list:
            stock_name_list = self.dynamicCall("GetMasterCodeName(QString)", code)
            self.condition_stock['code'].append(code)
            self.condition_stock['code_name'].append(stock_name_list)
            self.condition_stock['check_item'].append(0)
            self.condition_stock['check_daily_crawler'].append("0")
            self.condition_stock['check_min_crawler'].append("0")

        condition_df = DataFrame(self.condition_stock, columns=['code', 'code_name', 'check_item',
                                                                'check_daily_crawler', 'check_min_crawler'])

        dtypes = dict(zip(list(condition_df.columns), [Text] * len(condition_df.columns)))  # 모든 타입을 Text 로
        dtypes['check_item'] = Integer  # check_item 만 int 로 변경

        condition_df.to_sql('stock_item_all', self.engine_daily_buy_list, if_exists='replace', dtype=dtypes)
        self.condition_event_loop.exit()


#### 4.조건검색식 가져오는 함수 ####
self.open_api.get_condition()
################################


    def date_rows_setting(self):
        print("date_rows_setting!!")
        # 날짜 지정
        ############################ 7.조건검색 사용시 추가할 부분 #################################################
        sql = "select code_name from stock_item_all"
        item_code_name = self.engine_daily_buy_list.execute(sql).fetchall()

        for name in item_code_name:
            name = name[0]
            sql = "select code_name from %s where date < %s"
            self.basic_code_name = self.engine_daily_craw.execute(sql % (name, self.start_date)).fetchone()

            if self.basic_code_name:
                self.basic_code_name = self.basic_code_name[0]
                break

        sql = "select date from %s where date >= '%s' group by date"
        self.date_rows = self.engine_daily_craw.execute(sql % (self.basic_code_name, self.start_date)).fetchall()
        #########################################################################################################
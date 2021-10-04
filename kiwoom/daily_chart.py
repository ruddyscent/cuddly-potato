#!/usr/bin/env python3

import datetime
import time

from pykiwoom.kiwoom import *

INTERVAL = 3.6 # seconds

# 로그인
kiwoom = Kiwoom()
kiwoom.CommConnect()

# 전종목 종목코드
kospi = kiwoom.GetCodeListByMarket('0')
kosdaq = kiwoom.GetCodeListByMarket('10')
codes = kospi + kosdaq

# 문자열로 오늘 날짜 얻기
now = datetime.datetime.now()
today = now.strftime("%Y%m%d")

# 전 종목의 일봉 데이터
for i, code in enumerate(codes):
    print(f"{i + 1}/{len(codes)} {code}")
    df_list = []
    df = kiwoom.block_request("opt10081",
                              종목코드=code,
                              기준일자=today,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
    df_list.append(df)
    time.sleep(INTERVAL)

    while kiwoom.tr_remained:
        df = kiwoom.block_request("opt10081",
                                  종목코드=code,
                                  기준일자=today,
                                  수정주가구분=1,
                                  output="주식일봉차트조회",
                                  next=2)
        df_list.append(df)
        time.sleep(INTERVAL)

    dfs = pd.concat(df_list, ignore_index=True)
    out_name = f"{code}.csv"
    dfs.to_csv(out_name)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 21:13:14 2020

@author: stevenhsu
"""
import configuration as cf
import random
from steventricks.mighty import make_url,data_renew
from copy import deepcopy
from datetime import datetime
import pandas as pd
import numpy as np
now = datetime.now().date()
crawlerdic={
    "每日收盤行情":{
        "m":"market",
        "freq":"D",
        "dname":"date",
        "date_min":"2004-2-11",
        "payload":{"response": "json",
                   "date": "",
                   "type": "ALL",
                   "_": "1613296592078"},
        "url":r"https://www.twse.com.tw/exchangeReport/MI_INDEX?",
        "title":["market_"+_ for _ in ["價格指數(臺灣證券交易所)","價格指數(跨市場)","價格指數(臺灣指數公司)","報酬指數(臺灣證券交易所)","報酬指數(跨市場)","報酬指數(臺灣指數公司)","大盤統計資訊","漲跌證券數合計","每日收盤行情"]]
        },
    "信用交易統計":{
        "m":"market",
        "freq":"D",
        "dname":"date",
        "date_min":"2001-1-1",
        "payload":{"response": "json",
                   "date": "",
                   "selectType": "ALL"},
        "url":r"https://www.twse.com.tw/exchangeReport/MI_MARGN?",
        "title":["market_"+_ for _ in ["融資融券彙總","信用交易統計"]]
        },
    "市場成交資訊":{
        "m":"market",
        "freq":"M",
        "date_min":"1990-1-4",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "_":"1613392395864"},
        "url":r"https://www.twse.com.tw/exchangeReport/FMTQIK?",
        "title":["market_"+_ for _ in ["市場成交資訊"]]
        },
    "三大法人買賣金額統計表":{
        "m":"market",
        "freq":"D",
        "date_min":"2004-4-7",
        "dname":"dayDate",
        "payload":{"response": "json",
                   "dayDate": "",
                   "type": "day",
                   "_":"1613389589646"},
        "url":r"https://www.twse.com.tw/fund/BFI82U?",
        "title":["market_"+_ for _ in ["三大法人買賣金額統計表"]]
        },
    "三大法人買賣超日報":{
        "m":"market",
        "freq":"D",
        "date_min":"2012-5-2",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "selectType": "ALL"},
        "url":r"https://www.twse.com.tw/fund/T86?",
        "title":["market_"+_ for _ in ["三大法人買賣超日報"]]
        },
    "個股日本益比、殖利率及股價淨值比":{
        "m":"stock",
        "freq":"D",
        "date_min":"2012-5-2",
        "dname":"date",
        "payload":{"response": "json",
                  "date": "",
                  "selectType" : "ALL",
                  "_": "1596117278906"},
        "url":r"https://www.twse.com.tw/exchangeReport/BWIBBU_d?",
        "title":["stock_"+_ for _ in ["個股日本益比、殖利率及股價淨值比"]]
        },
    "信用額度總量管制餘額表":{
        "m":"stock",
        "freq":"D",
        "date_min":"2005-7-1",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "_": "1596721575815"},
        "url":r"https://www.twse.com.tw/exchangeReport/TWT93U?",
        "title":["stock_"+_ for _ in ["信用額度總量管制餘額表"]]
        },
    "當日沖銷交易標的及成交量值":{
        "m":"stock",
        "freq":"D",
        "date_min":"2014-1-6",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "selectType": "All",
                   "_": "1596117305431"},
        "url":r"https://www.twse.com.tw/exchangeReport/TWTB4U?",
        "title":["stock_"+_ for _ in ["當日沖銷交易標的及成交量值"]]
        },
    # 這裡的,"當日沖銷交易統計"跟market有重複，因為都是大盤的沖銷交易===========
    "每月當日沖銷交易標的及統計":{
        "m":"market",
        "freq":"M",
        "date_min":"2014-1-6",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "stockNo": "",
                   "_": "1596117360962"},
        "url":"https://www.twse.com.tw/exchangeReport/TWTB4U2?",
        "title":["market_"+_ for _ in ["每月當日沖銷交易標的及統計"]]
        },
    "外資及陸資投資持股統計":{
        "m":"stock",
        "freq":"D",
        "date_min":"2004-2-11",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "selectType": "ALLBUT0999",
                   "_": "1594606204191"},
        "url":"https://www.twse.com.tw/fund/MI_QFIIS?",
        "title":["stock_"+_ for _ in ["外資及陸資投資持股統計","外資投資持股統計"]]
        },
    "發行量加權股價指數歷史資料":{
        "m":"market",
        "freq":"D",
        "date_min":"1999-1-5",
        "dname":"date",
        "payload":{"response": "json",
                   "date": "",
                   "_": "1597539490294"},
        "url":"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?",
        "title":["market_"+_ for _ in ["發行量加權股價指數歷史資料"]]
        },
        }

stocktable={"url":r"https://isin.twse.com.tw/isin/C_public.jsp?strMode={}",
            "charset":"cp950"
            }

headers={"safari14.0":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
         "iphone13":"Mozilla/5.0 (iPhone; CPU iPhone OS 13_1_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
         "ipod13":"Mozilla/5.0 (iPod; CPU iPhone OS 13_1_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
         "ipadmini13":"Mozilla/5.0 (iPad; CPU iPhone OS 13_1_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
         "ipad":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Safari/605.1.15",
         "winedge":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
         "mac":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
         "chromewin":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
         "firefoxmac":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:70.0) Gecko/20100101 Firefox/70.0",
         "firefoxwin":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0"}

def iter_headers(header=headers):
    lis=[x for x in header.values()]
    while True :
        r=random.randint(0,len(lis)-1)
        yield {"User-Agent":lis[r]}

def datesplit(d, sp="-"):
    d = d.split(sp)
    return d[0] + d[1].zfill(2) + d[2].zfill(2)

def stocktablecrawl(maxn=12,timeout=180):
    res=[]
    for _ in range(1,maxn,1):
        res.append(make_url(url=stocktable["url"].format(_),timeout=timeout,typ="html",charset=stocktable["charset"]))
    print(res)
    res = [_ for _ in res for _ in _]
    res1=pd.DataFrame()
    for i in res:
        i.set_index("國際證券辨識號碼(ISIN Code)",inplace=True)
        i.loc[:,["代號","名稱"]]=i.iloc[:,0].str.split("　| ",expand=True).rename(columns={0:"代號",1:"名稱"})
        i.drop_duplicates(inplace=True)
        res1=data_renew(res1,i)
    return res1

def multilisforcrawl(itemlis=[],crawldic=crawlerdic):
    # print(itemlis)
    res=[]
    for i in itemlis :
        date  = i[0]
        item  = i[1]
        if item not in crawldic : continue

        crawl = deepcopy(crawldic[item])
        dname = crawl["dname"]
        crawl["payload"][dname] = datesplit(date)
        crawl["item"] = item
        crawl["crawldate"] = date
        crawl["header"] = next(iter_headers())
        res.append(crawl)
    return res

def crawlerdictodf(defaultstr="wait"):
    def makeseries(col="",start="",end=now,freq="",pendix="",defaultstr=defaultstr):
        d = pd.date_range(start=start, end=end, freq=freq)
        d = d.append(pd.DatetimeIndex([now]))
        d = d.unique()
        s = pd.Series(np.repeat(defaultstr,d.size), index=d, name=pendix+col)
        return s
    reslis = []

    for key in crawlerdic :
            for title in crawlerdic[key]["title"]:
                reslis.append(makeseries(col=title,start=crawlerdic[key]["date_min"],freq=crawlerdic[key]["freq"],pendix=""))

    return pd.concat(reslis,axis=1)


if __name__ == '__main__' :
    l=[]
    # l = crawlerdictodf()
    # l.loc[None:"2020-4",["i每日收盤行情"]]

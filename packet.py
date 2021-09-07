#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 21:13:14 2020

@author: stevenhsu
"""
import configuration as cf
import random
from steventricks.mighty import make_url
from steventricks.db import dbmanager
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
        "title":["價格指數(臺灣證券交易所)","價格指數(跨市場)","價格指數(臺灣指數公司)","報酬指數(臺灣證券交易所)","報酬指數(跨市場)","報酬指數(臺灣指數公司)","大盤統計資訊","漲跌證券數合計","每日收盤行情"]
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
        "title":["融資融券彙總","信用交易統計"]
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
        "title":["市場成交資訊"]
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
        "title":["三大法人買賣金額統計表"]
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
        "title":["三大法人買賣超日報"]
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
        "title": ["個股日本益比、殖利率及股價淨值比"]
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
        "title":["信用額度總量管制餘額表"]
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
        "title":["當日沖銷交易標的及成交量值","當日沖銷交易統計資訊"]
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
        "title":["每月當日沖銷交易標的及統計"]
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
        "title":["外資及陸資投資持股統計","外資投資持股統計"]
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
        "title": ["發行量加權股價指數歷史資料"]
        },
    }

title_dic = {
    t:_ for _ in crawlerdic for t in crawlerdic[_]["title"]
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

rename_dic={
    "上市認購(售)權證"                       :"上市認購售權證",
    "臺灣存託憑證(TDR)"                    :"臺灣存託憑證",
    "受益證券-不動產投資信託"             :"受益證券_不動產投資信託",
    "國際證券辨識號碼(ISIN Code)"     :"ISINCode",
    "上櫃認購(售)權證"                       :"上櫃認購售權證",
    "受益證券-資產基礎證券"                :"受益證券_資產基礎證券",
    "黃金期貨(USD)"                          :"黃金期貨",
    "成交金額(元)"                             :"成交金額_元",
    "成交股數(股)"                             :"成交股數_股",
    "漲跌百分比(%)"                           :"漲跌百分比%",
    "自營商買進股數(自行買賣)"           :"自營商買進股數_自行買賣",
    "自營商賣出股數(自行買賣)"           :"自營商賣出股數_自行買賣",
    "自營商買賣超股數(自行買賣)"        :"自營商買賣超股數_自行買賣",
    "自營商買進股數(避險)"                 :"自營商買進股數_避險",
    "自營商賣出股數(避險)"                 :"自營商賣出股數_避險",
    "自營商買賣超股數(避險)"               :"自營商買賣超股數_避險",
    "殖利率(%)"                                 :"殖利率%",
    "外陸資買進股數(不含外資自營商)"   :"外陸資買進股數_不含外資自營商",
    "外陸資賣出股數(不含外資自營商)"   :"外陸資賣出股數_不含外資自營商",
    "外陸資買賣超股數(不含外資自營商)":"外陸資買賣超股數_不含外資自營商",
    "現金(券)償還":"現金券償還",
    "證券代號":"代號",
    "證券名稱":"名稱",
    "股票代號":"代號",
    "指數代號":"代號",
    "股票名稱":"名稱",
    "有價證券名稱":"名稱",
    
    }
# z=[[1,2,3],[5,5,5],[6],[1,1]]
# zz=[_ for _ in z if len(set(_))==1 for _ in set(_)]
# [(a,b) for a,b in zip(zz,zz[1:]+[None])]
# zzz=pd.DataFrame([[2,2,2],[4,4,4],[7,8,9],[10,11,12],[13,14,15],[16,17,18]],index=["a","b","c","d","e","f"])
# zzz.loc[zzz.index=="a",:].values
# zzz.values
# zzz["d"]=cf.now
# zzz.dtypes

def stocktablecrawl(maxn=13,timeout=180,pk="ISINCode"):
    dm = dbmanager(root=cf.cloud_path,db="stocktable")
    for _ in range(1,maxn,1):
        df=make_url(url=stocktable["url"].format(_),timeout=timeout,typ="html",charset=stocktable["charset"])
        df = pd.DataFrame(df[0])
        if df.empty is True :
            print("stocktable No:{} ___empty crawled result".format(_))
            continue
        df=df.reset_index(drop=True).reset_index()
        tablename= [list(set(_)) for _ in df.values if len(set(_))==2]
        df.drop(["index","Unnamed: 6"],errors="ignore",axis=1,inplace=True)
        df.loc[:,"date"]=pd.to_datetime(cf.now)
        if "指數代號及名稱" in df:
            df.loc[:,["代號","名稱"]] = df.loc[:,"指數代號及名稱"].str.split(" |　",expand=True,n=1).rename(columns={0:"代號",1:"名稱"})
        elif "有價證券代號及名稱" in df:
            df.loc[:,["代號","名稱"]] = df.loc[:,"有價證券代號及名稱"].str.split(" |　",expand=True,n=1).rename(columns={0:"代號",1:"名稱"})
        df = df.rename(columns=rename_dic)
        if pk  not in df :
            print("no primary key")
            print(_)
            continue
        if len(tablename)>1:
            name_index=[(a,b) for a,b in zip(tablename,tablename[1:]+[[None]])]
        elif len(tablename)==1:
            name_index = [(tablename[0], [None])]
        else:
            table="無細項分類的商品{}".format(str(_))
            df.loc[:, "product"] = table
            dm.to_sql_ex(df=df,table=table,pk=pk)
            continue
        
        for nameindex in name_index:
            start=nameindex[0]
            end=nameindex[1]
            startname,startint=[_ for _ in start if isinstance(_,str) is True][0],[_ for _ in start if isinstance(_,str) is False][0]
            endint=[_ for _ in end if isinstance(_,str) is False][0]
            if end[0] is None:
                df_sub = df[startint+1:]
            else:
                df_sub = df[startint+1:endint]
                
            if startname in rename_dic : startname = rename_dic[startname]
            df_sub.loc[:,"product"]=startname
            dm.to_sql_ex(df=df_sub,table=startname,pk=pk)

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

def crawlerdictodf(defaultstr="wait",typ="item"):
    def makeseries(col="",start="",end=now,freq="",pendix="",defaultstr=defaultstr):
        d = pd.date_range(start=start, end=end, freq=freq)
        d = d.append(pd.DatetimeIndex([now]))
        d = d.unique()
        s = pd.Series(np.repeat(defaultstr,d.size), index=d, name=pendix+col)
        return s
    reslis = []
    if typ == "item":
        for key in crawlerdic :
            reslis.append(makeseries(col=key, start=crawlerdic[key]["date_min"], freq=crawlerdic[key]["freq"], pendix=""))
    elif typ == "title":
        for key in crawlerdic:
            for title in crawlerdic[key]["title"]:
                reslis.append(makeseries(col=title,start=crawlerdic[key]["date_min"],freq=crawlerdic[key]["freq"],pendix=""))
    else:
        print("wrong typ arg !")
        return

    return pd.concat(reslis,axis=1)

def get_item(title):
    return title_dic[title]

def get_title(item):
    return crawlerdic[item]["title"]

def search_title(item,title):
    res=[_ for _ in crawlerdic[item]["title"] if _ in title]
    return res

if __name__ == '__main__' :
    l=[]
    # l = crawlerdictodf()
    # l.loc[None:"2020-4",["i每日收盤行情"]]

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 22 23:22:32 2020

@author: mac
"""
import configuration as cf
from os import path, walk
from steventricks.mighty import picklesave, pickleload, path_walk,data_renew, make_url, dataframe_zip
import requests as re
from datetime import datetime
from packet import crawlerdic, crawlerdictodf, multilisforcrawl, stocktablecrawl
import pandas as pd
from multiprocessing import Pool
from traceback import format_exc
import sys
now = datetime.now().date()

def parser(crawlerdic={},timeout=20 ):
# example : date == "2020-3-10"
    crawldate = crawlerdic["crawldate"]
    item         = crawlerdic["item"]
    url           = crawlerdic["url"]
    header      = crawlerdic["header"]
    payload    = crawlerdic["payload"]
    m               = crawlerdic["m"]
    
# debug for date =====================================================
    try:
        if pd.isnull(pd.Timestamp(crawldate)) == True :
            crawlerdic["errormessage"] = "get None date value"
            crawlerdic["stat"] = "inputdateerror"
            return crawlerdic
    except ValueError as e :
        crawlerdic["errormessage"] = format_exc() + e
        crawlerdic["stat"] = "inputdateerror"
        return crawlerdic
    except:
        crawlerdic["errormessage"] = format_exc()
        crawlerdic["stat"] = "othererrors"
        return crawlerdic

# debug for url =====================================================
    link = make_url(url = url, data = payload, headers = header,typ="post",timeout=timeout)
    if isinstance(link, str) == True :
        crawlerdic["errormessage"] = link
        crawlerdic["stat"] = "badconnection"
        return crawlerdic
    elif pd.isnull(link) == True :
        crawlerdic["errormessage"] = link
        crawlerdic["stat"] = "badconnection"
        return crawlerdic
    elif link.status_code != re.codes.ok :
        crawlerdic["errormessage"] = link.status_code
        crawlerdic["stat"] = "badconnection"
        return crawlerdic
    
# debug for jsontext ================================================
    try:
        jsontext = link.json()
    except:
        crawlerdic["errormessage"] = format_exc()
        crawlerdic["data"] =link
        crawlerdic["stat"] = "jsonerror"
        return crawlerdic

    if jsontext["stat"] != "OK" :
        crawlerdic["errormessage"] = jsontext["stat"]
        crawlerdic["data"] = jsontext
        crawlerdic["stat"] = "closed"
        return crawlerdic

# prepare for saving ================================================
    link.close()
    jsontext["item"]      = item
    jsontext["crawldate"] = crawldate
    jsontext["m"]         = m
    savepath              = path.join(cf.cloud_path, r"warehouse", item,
                                      str(pd.to_datetime(crawldate).year),
                                      "{}_{}".format(item,crawldate))
    picklesave(savepath,jsontext,cover=True)
    
    crawlerdic["data"]    = jsontext
    crawlerdic["stat"]    = str(datetime.now().date())
    return crawlerdic

# market == stock > item > title
class management(object):
    log_path            = path.join(cf.cloud_path, "log.pkl")
    warehouse_path = path.join(cf.cloud_path, "warehouse")
    stocktable         = pickleload(path.join(cf.cloud_path, r"stocktable.pkl"))
    log                     = crawlerdictodf()
    mall                   = set([_["m"] for _ in crawlerdic.values()])
    item                   = [_ for _ in crawlerdic]
    # titleall       = [i for i in crawldic.values() for i in i["title"]]

    def __init__(self, start=None, end=None):
        self.start = start
        self.end   = end
        self.log   = self.log.loc[self.start:self.end:]
        print(r"Renewing the log ... ")
        if path.exists(self.log_path) is True:
            old = pickleload(self.log_path)
            self.log.loc[:, self.item] = data_renew(self.log.loc[:, self.item],old.loc[:,self.item])
        
    def clearner_lis(self, item=[]):
        if isinstance(item, list) is False:
            print("Input error, item must be list")
            return None
        elif not item: item = self.crawldic.keys()
        p = self.warehouse_path
        res = []
        for k in item:
            temp = next(walk(path.join(p, self.crawldic[k]["m"], k)))[2]
            if not temp: continue
            res += temp
        return res
    
    def findstock(self,io=r""):
        io=io.split("_")
        if len(io)==2:
            temp = self.stocktable.loc[(self.stocktable["代號"]==io[0]) & (self.stocktable["名稱"]==io[1]),:]
        elif len(io)==1:
            temp = self.stocktable.loc[(self.stocktable["名稱"]==io[0]),:]
        else:
            return io
        if temp.empty == True : return None
        return temp["代號"]+"_"+temp["名稱"]

# In[]
if __name__ == "__main__":
    # a=pickleload(path.join(cf.cloud_path, "log.pkl"))
    stocktable_renew = False
    m = management()
    m.mall
    log = m.log
    crawldata = dataframe_zip(df=log, col_include=m.item, key_include=["wait"], time=False)
    # "badconnection","closed","jsonerror"
    multilis = multilisforcrawl(crawldata)
    if stocktable_renew == True:
        stocktable = stocktablecrawl(timeout=60)
        picklesave(path.join(cf.cloud_path, "stocktable.pkl"), stocktable, repl=True)
    # In[]
    try:
        for payload in multilis:
            print("crawling {}_{}".format(payload["crawldate"],payload["item"]))
            res = parser(payload)
            if "errormessage" in res:
                print(res["stat"])
                print(res["errormessage"])
            elif "date" in res["data"]:
                print("資料日期 ==> ", res["data"]["date"])
            else:
                print(res["stat"])
            
            log.loc[log[res["item"]].index == res["crawldate"], res["item"]] = res["stat"]
            print(log.loc[log[res["item"]].index == res["crawldate"], res["item"]])
            picklesave(m.log_path, log, cover=True)
            print("Log renewed .")
        # break
    except KeyboardInterrupt:
        picklesave(m.log_path, log, cover=True)
        print("KeyboardInterrupt ... content saving")
        print("Log saved .")
        sys.exit()
    except Exception as e:
        print("===============")
        print(format_exc())
        print("Unknowned error")
        print(e)
        picklesave(m.log_path, log, cover=True)
        print("Log saved .")
        sys.exit()

# a = pickleload(r"/Users/stevenhsu/Documents/GitHub/trading/log.pkl")
# a=a.iloc[:,:11]
# picklesave(r"/Users/stevenhsu/Documents/GitHub/trading/log.pkl",a,cover=True)

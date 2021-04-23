#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 22 23:22:32 2020

@author: mac
"""
import configuration as cf
import sys
import shutil
from os import path,walk
# sys.path.insert(0,path.dirname(__file__))
from steventricks.file import picklesave,pickleload,path_walk,data_renew,make_url,dataframe_zip
from steventricks.date import datefromstr,istime,datefromsplit
from steventricks.value import turntofloat,dfappend
from traceback import format_exc
# print(__main__)
# print(__package__)
# print(sys.path)
import requests as re
import pandas as pd
import numpy as np
# import json
from datetime import datetime
from packet import crawlerdic,crawlerdictodf
from multiprocessing import Pool
now = datetime.now().date()

def show_info(Path="", data=None):
    if path.exists(Path) == False :
        print(Path)
        print(r"No olderlog")
    else:
        print(r"OlderLog Information ============")
        print(r"Created in ",pd.to_datetime(path.getctime(Path),unit="s"))
        print(r"Last modified in ",pd.to_datetime(path.getmtime(Path),unit="s"))
        if data is None : data = pickleload(Path)
        print(r"There are data existing through out {b} to {e}".format(b=data.index.min().date(),e=data.index.max().date()))
        print("OlderLog Information End ========")
    return

def parser(crawlerdic={},timeout=180 ):
# example : date == "2020-3-10"
    crawldate = crawlerdic["crawldate"]
    item      = crawlerdic["item"]
    url       = crawlerdic["url"]
    header    = crawlerdic["header"]
    payload   = crawlerdic["payload"]
    m         = crawlerdic["m"]
    
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
    savepath              = path.join(cf.cloud_path, r"warehouse", item,"{}_{}".format(item,crawldate))
    picklesave(savepath,jsontext,repl=True)
    
    crawlerdic["data"]    = jsontext
    crawlerdic["stat"]    = str(datetime.now().date())
    return crawlerdic

# In[]

class management(object):
    olderlog_path  = path.join(cf.cloud_path,"log.pkl")
    warehouse_path = path.join(cf.cloud_path,"warehouse")
    olderlog_file  = pickleload(olderlog_path)
    stocktable     = pickleload(path.join(cf.cloud_path,r"stocktable.pkl"))
    crawldic       = crawlerdic
    log            = crawlerdictodf(title="sim")
    mall           = set([ _["m"] for _ in crawldic.values() ])
    itemall        = [_ for _ in crawldic ]
    titleall       = [i for i in crawldic.values() for i in i["title"]]

    def __init__(self,start=None ,end=None, loginit=False) :
        self.start = start
        self.end   = end
        self.log   = self.log.loc[self.start:self.end,:]
        # print(self.log)
        print(r"Renewing the log ... ")
        if loginit == True :
            self.log.loc[:,self.itemall] = data_renew(self.log.loc[:,self.itemall],self.olderlog_file)
            for _ in self.mall:
                shutil.rmtree(path.join(self.warehouse_path, _, r"product"),ignore_errors=True)
        elif loginit == False:
            self.log = data_renew(self.log,self.olderlog_file)
        show_info(self.olderlog_path,self.olderlog_file)
    
    def get_item(self, item=[] ,title="sim"):
        item = [_ for _ in item if _ in self.mall]
        if not item : item = self.mall
        res = [_ for _ in self.crawldic if self.crawldic[_]["m"] in item]
        t = [_ for _ in res for _ in self.crawldic[_]["title"]]
        if title =="sim" :
            return res + t
        elif title == "only" :
            return t
        elif title == False:
            return res
        else :
            print("wrong title string")
            return None
        
    def dataforparse(self,key_include=[],key_exclude=[],col_include=[],col_exclude=[],time=False):
        return dataframe_zip(df=self.log,col_include=col_include,col_exclude=col_exclude,key_include=key_include,key_exclude=key_exclude,time=time)

    def clearner_lis(self, item=[]):
        if isinstance(item, list) == False : 
            print("Input error, item must be list.")
            return None
        elif not item : item = self.crawldic.keys()
        p = self.warehouse_path
        res=[]
        for k in item:
            temp = next(walk(path.join(p, self.crawldic[k]["m"], k)))[2]
            if not temp : continue
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
if __name__ == "__main__" :
    # m=management()
    # a=m.olderlog_file
    pass
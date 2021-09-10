#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 01:23:03 2020

@author: stevenhsu
"""
import configuration as cf
import gc
import sys
from os.path import join,exists
from traceback import format_exc
import pandas as pd
from steventricks.mighty import pickleload, picklesave,  turntofloat, df_append, path_walk, fileload, roctoad, isnumber
from packet import  search_title,rename_dic,stocktable_combine
from steventricks.db import dbmanager 

key_dict = {
    "col": ["field"],
    "value": ["data", "list"],
    "title": ["title"]
}

def getkeys(data):
    product = {
        "col"  : [],
        "value": [],
        "title": [],
    }
    for key in sorted(data.keys()):
        for k, i in key_dict.items():
            i = [key for _ in i if _ in key.lower()]
            if i: product[k] += i
    return pd.DataFrame(product)

errordict = {
    "titlename_error":{},
    "newtitle"       :{},
    "error"          :{},
    "logisnotwait"   :{},
    "no_data"        :{},
            }

class logmanagement(object):
    def __init__(self,log="title_log.pkl"):
        self.log_path=join(cf.cloud_path,log)
        if exists(self.log_path) is True:
            self.log = fileload(self.log_path)[0][1]
        elif exists(self.log_path) is False:
            self.log = {}
    def log_exists(self,key="",value=""):
        if key in self.log : 
            if value in self.log[key]:
                return True
        return False
    def log_append(self,key="",value=""):
        if key not in self.log: self.log[key]=[]
        if key in self.log    : self.log[key].append(value)
        
gc.disable()
debug=False
m=logmanagement()
m.log

db = dbmanager(root=cf.cloud_path,db="stocktable")
stocktable = db.alltableget(filename="stocktable")
stocktable.index = stocktable["代號"].str.strip() + "_" + stocktable["名稱"].str.strip()
stocktable = stocktable.loc[:,["product"]]
data_dir=path_walk(join(cf.cloud_path,"warehouse"),dir_include=["信用額度總量管制"],file_include=[".pkl"])
for file_path in data_dir["path"]:
    data = fileload(file_path)[0]
    filename,data=data[0],data[1]
    item,crawldate = filename.split("_")
    product = getkeys(data)
    print(item,crawldate+"===================================")
    
    for col,value,title in product.values[:] :
        print(col,value,title)
        value = data[value]
# 資料是空的就直接排除=========================================
        if not value : continue
# 找出正確的title放到savename==================================
        print(data["title"])
        title= search_title(item=item,title=data[title])
        if len(title) == 1 :
            title=title[0]
        elif len(title)>1:
            print(title)
            if item not in errordict["titlename_error"]:errordict["titlename_error"][item] = ["{}_{}".format(crawldate,",".join(title))]
            errordict["titlename_error"][item].append("{}_{}".format(crawldate,",".join(title)))
            continue
        elif not title:
            print("new title !!!    ",data[title])
            if item not in errordict["newtitle"]:errordict["newtitle"][item] = ["{}_{}".format(crawldate,",".join(title))]
            errordict["newtitle"][item].append("{}_{}".format(crawldate,",".join(title)))
            continue
        else :
            if item not in errordict["error"]:errordict["error"][item] = ["{}_{}".format(crawldate,",".join(title))]
            errordict["error"][item].append("{}_{}".format(crawldate,",".join(title)))
            print("unexpected error")
            continue
# 沒有在titlezip代表log裡面已經有資料了，不用再找第二次，所以跳出
        if m.log_exists(key=title,value=filename) is True:continue
        col = data[col]
        df = pd.DataFrame(value)
# 欄位排序有錯的直接把非文字的數值排除掉========================
        if df.columns.size != len(col) :
            df = df.T
            for c in df.columns :
                if df[c].dropna().size != len(col) :
                    df[c] = df[c].apply(lambda x : x if isinstance(x, str) == True else None)
                    df[c] = df[c].dropna().reset_index(drop=True)
            df = df.dropna().T
        # 開始進行欄位清理============================================================
        # 所有類型都會碰到的清理方式===================================
        df.columns = [ str(_).replace("</br>","") for _ in col]
        df.drop("前日餘額",axis=1,inplace=True,errors="ignore")
        df.replace(",","",regex=True,inplace=True)
        df = df.rename(columns=rename_dic)
        df.loc[:,"date"]=pd.to_datetime(crawldate)
        
        if "融資融券" in title :
            df.columns = ",".join(df.columns).replace("買進","融券買進").replace("融券買進","融資買進",1).split(",")
            df.columns = ",".join(df.columns).replace("賣出","融券賣出").replace("融券賣出","融資賣出",1).split(",")
            df.columns = ",".join(df.columns).replace("今日餘額","今日融券餘額").replace("今日融券餘額","今日融資餘額",1).split(",")
            df.columns = ",".join(df.columns).replace("限額","融券限額").replace("融券限額","融資限額",1).split(",")
        elif "信用額度總量管制餘額表" in title:
            if "當日賣出" in df:
                df.columns = ",".join(df.columns).replace("賣出","融券賣出").replace("買進","融券買進").split(",")
            else:
                df.columns = ",".join(df.columns).replace("賣出", "當日賣出").replace("買進", "融券買進").replace("當日賣出","融券賣出",1).split(",")
            if "當日餘額" not in df:
                df.columns = ",".join(df.columns).replace("今日餘額", "當日餘額").replace("當日餘額", "今日餘額",1).split(",")
            
        unit=1
        if title in ["信用交易統計","融資融券彙總"]:
            unit=1000
        df = turntofloat(df,col=["成交股數","成交筆數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比","今日餘額","買進","賣出","融資買進","融資賣出","融券買進","融券賣出","現金償還","今日融資餘額","今日融券餘額","融資限額","融券限額","現券償還","資券互抵","成交金額_元","成交股數_股","現金券償還","當日賣出","當日還券","當日調整","當日餘額","次一營業日可限額","發行股數","外資及陸資尚可投資股數","全體外資及陸資持有股數","外資及陸資尚可投資比例","全體外資及陸資持股比率","外資及陸資共用法令投資上限比率","陸資法令投資上限比率","外資尚可投資股數","全體外資持有股數","外資尚可投資比率","全體外資持股比率","法令投資上限比率","開盤指數","最低指數","最高指數","收盤指數","漲跌點數","漲跌百分比%","發行量加權股價指數","買進金額","賣出金額","買賣差額","外資買進股數","外資賣出股數","外資買賣超股數","投信買進股數","投信賣出股數","投信買賣超股數","自營商買賣超股數","自營商買進股數_自行買賣","自營商賣出股數_自行買賣","自營商買賣超股數_自行買賣","自營商買進股數_避險","自營商賣出股數_避險","自營商買賣超股數_避險","三大法人買賣超股數","殖利率%","股價淨值比","當日沖銷交易總成交股數","當日沖銷交易總成交股數占市場比重%","當日沖銷交易總買進成交金額","當日沖銷交易總買進成交金額占市場比重%","當日沖銷交易總賣出成交金額","當日沖銷交易總賣出成交金額占市場比重%","當日沖銷交易成交股數","當日沖銷交易買進成交金額","當日沖銷交易賣出成交金額","外陸資買進股數_不含外資自營商","外陸資賣出股數_不含外資自營商","外陸資買賣超股數_不含外資自營商","外資自營商買進股數","外資自營商賣出股數","外資自營商買賣超股數","自營商買進股數","自營商賣出股數"],unit=unit)
        # 變更index================================================
        print(df)
        if "代號" in df and "名稱" in df :
            df.index = df["代號"].str.strip()+"_"+df["名稱"].str.strip()
            pk="date"
        elif "日期" in df :
            df.loc[:,"日期"] = df["日期"].map(lambda x : roctoad(x,method="realtime"))
        # df.index.name="index"
        if "信用額度總量管制" in title:
            df = df.iloc[:,6:]
        if "最近一次上市公司申報外資持股異動日期" in df:
            df.loc[:, "最近一次上市公司申報外資持股異動日期"] = df["最近一次上市公司申報外資持股異動日期"].map(lambda x: roctoad(x, method="realtime"))
        if "最近一次上市公司申報外資及陸資持股異動日期" in df:
            df.loc[:, "最近一次上市公司申報外資及陸資持股異動日期"] = df["最近一次上市公司申報外資及陸資持股異動日期"].map(lambda x: roctoad(x, method="realtime"))
        if "財報年/季" in df:
            df.loc[:, "財報年/季"] = df["財報年/季"].map(lambda x: roctoad(x, method="realtime"))
        
        try:
# 開始分為stock 和 market兩種方式來儲存==========================
            print(df)
            print(title)
            print(item)
            print(df.columns)
            if title in ["三大法人買賣金額統計表","信用交易統計","價格指數(臺灣證券交易所)","價格指數(跨市場)","價格指數(臺灣指數公司)","報酬指數(臺灣證券交易所)","報酬指數(跨市場)","報酬指數(臺灣指數公司)","大盤統計資訊","漲跌證券數合計"]:
                db.database_change(root=join(cf.cloud_path, "warehouse"), newdatabase="market")
                db.to_sql_ex(df=df, table=title)
                
            elif title in ["當日沖銷交易統計資訊"]:
                db.database_change(root=join(cf.cloud_path, "warehouse"), newdatabase="market")
                db.to_sql_ex(df=df, table=title, pk="date")
                
            elif "日期" in df:
                db.database_change(root=join(cf.cloud_path, "warehouse"), newdatabase="market")
                db.to_sql_ex(df=df, table=title, pk="日期")

            else :
                df = stocktable_combine(df=df, stocktable=stocktable)
                for product in df["product"].unique():
                    if pd.isnull(product) is True : product = "無細項分類商品"
                    db.database_change(root=join(cf.cloud_path,"warehouse"),newdatabase=product)
                    for stock in df.loc[df["product"]==product,:].index:
                        print("\r{}".format(stock),end="")
                        newdf = df.loc[df.index.isin([stock]),:]
                        db.to_sql_ex(df=newdf.drop("product",axis=1),table=stock,pk=pk)
        
# 存檔完成進行log改寫=========================================
            m.log_append(key=title,value=filename)
            picklesave(m.log_path,m.log,cover=True)
            print("saving sucessed !!")
            gc.collect()
        except KeyboardInterrupt:
            print("KeyboardInterrupt ... content saving canceled !")
            sys.exit()
        except Exception as e :
            print("===============")
            print(format_exc())
            print("Unknowned error")
            print(e)
            break
            # sys.exit()
    break

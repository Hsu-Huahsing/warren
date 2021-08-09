#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 01:23:03 2020

@author: stevenhsu
"""
import configuration as cf
import gc
import sys
from os import path
from traceback import format_exc
import pandas as pd
from steventricks.mighty import pickleload, picklesave,  turntofloat, df_append
from packet import crawlerdictodf
class management(object):
    def __init__(self,log="title_log.pkl"):
        self.log_path=path.join(cf.cloud_path,log)
        if path.exists(self.log_path) is True:
            self.log = pickleload(self.log_path)
        elif path.exists(self.log_path) is False:
            self.log = crawlerdictodf(typ="title")

gc.disable()
debug=False
m=management()
m.log
itemcol = m.get_item(title=False)
itemzip = m.dataforparse(col_include=itemcol,time=True)

key_dict = {
    "col"  :["field"],
    "value":["data","list"],
    "title":["title"]
            }

errordict = {
    "list>1"      :{},
    "newtitle"    :{},
    "error"       :{},
    "logisnotwait":{},
             }

if __name__ == '__main__' :
    for d,f in itemzip :
        data = pickleload(path.join(m.warehouse_path,f,"{f}_{d}".format(f=f,d=d)))
        product = {
            "col"  :[],
            "value":[],
            "title":[],
            }
        # crawldic  = m.crawldic[data["item"]]
        crawldate = data["crawldate"]
        item      = data["item"]
        
        for key in  sorted(data.keys()):
            for k,i in key_dict.items() :
                i=[key for _ in i if _ in key.lower()]
                if i: product[k] += i
        print(item,crawldate+"===================================")
        for col,value,title in pd.DataFrame(product).values[:] :
            print(col,value,title)
            value = data[value]
    # 資料是空的就直接排除=========================================
            if not value : continue
    # 找出正確的title放到savename==================================
            t= [_ for _ in m.titleall if _.split("_",1)[1] in data[title]]
            if len(t) == 1 :
                t=t[0]
                savename = item+"_"+t+"{s}.pkl"
                print(savename)
            elif len(t)>1:
                print(t)
                if item not in errordict["list>1"]:errordict["list>1"][item] = [str(crawldate)+data[title]]
                errordict["list>1"][item].append(str(crawldate)+data[title])
                continue
            elif not t:
                print("new title !!!    ",data[title])
                if item not in errordict["newtitle"]:errordict["newtitle"][item] = [str(crawldate)+data[title]]
                errordict["newtitle"][item].append(str(crawldate)+data[title])
                # sys.exit()
                continue
            else :
                if item not in errordict["error"]:errordict["error"][item] = [str(crawldate)+data[title]]
                errordict["error"][item].append(str(crawldate)+data[title])
                print("unexpected error")
                continue
                # sys.exit()
    # 沒有在titlezip代表log裡面已經有資料了，不用再找第二次，所以跳出
            if m.log.loc[crawldate,t] != "wait" :
                if item not in errordict["logisnotwait"]:errordict["logisnotwait"][item] = [str(crawldate)+data[title]]
                errordict["logisnotwait"][item].append(str(crawldate)+data[title])
                print(crawldate,t)
                print("log is not wait")
                continue
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
            df.replace(",","",regex=True,inplace=True)
            df = turntofloat(df,col=["成交股數","成交筆數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比","買進","賣出","前日餘額","現金償還","今日餘額","限額","現券償還","資券互抵","成交金額(元)","成交股數(股)","現金(券)償還","當日賣出","當日還券","當日調整","當日餘額","次一營業日可限額","發行股數","外資及陸資尚可投資股數","全體外資及陸資持有股數","外資及陸資尚可投資比例","全體外資及陸資持股比率","外資及陸資共用法令投資上限比率","陸資法令投資上限比率","外資尚可投資股數","全體外資持有股數","外資尚可投資比率","全體外資持股比率","法令投資上限比率","開盤指數","最低指數","最高指數","收盤指數","漲跌點數","漲跌百分比(%)","發行量加權股價指數","買進金額","賣出金額","買賣差額","外資買進股數","外資賣出股數","外資買賣超股數","投信買進股數","投信賣出股數","投信買賣超股數","自營商買賣超股數","自營商買進股數(自行買賣)","自營商賣出股數(自行買賣)","自營商買賣超股數(自行買賣)","自營商買進股數(避險)","自營商賣出股數(避險)","自營商買賣超股數(避險)","三大法人買賣超股數","殖利率(%)","股價淨值比","當日沖銷交易總成交股數","當日沖銷交易總成交股數占市場比重%","當日沖銷交易總買進成交金額","當日沖銷交易總買進成交金額占市場比重%","當日沖銷交易總賣出成交金額","當日沖銷交易總賣出成交金額占市場比重%","當日沖銷交易成交股數","當日沖銷交易買進成交金額","當日沖銷交易賣出成交金額","外陸資買進股數(不含外資自營商)","外陸資賣出股數(不含外資自營商)","外陸資買賣超股數(不含外資自營商)","外資自營商買進股數","外資自營商賣出股數","外資自營商買賣超股數"])
            # 變更index================================================
            if "證券代號" in df and "證券名稱" in df :
                df.index = df["證券代號"].str.strip()+"_"+df["證券名稱"].str.strip()
                df.drop(["證券代號","證券名稱"],axis=1,inplace=True)
            elif "股票代號" in df and "股票名稱" in df :
                df.index = df["股票代號"].str.strip()+"_"+df["股票名稱"].str.strip()
                df.drop(["股票代號","股票名稱"],axis=1,inplace=True)
            elif "成交統計" in df :
                df.index = df["成交統計"].str.strip()
                df.drop("成交統計",axis=1,inplace=True)
            elif "項目" in df:
                df.index = df["項目"].str.strip()
                df.drop("項目",axis=1,inplace=True)
            elif "單位名稱" in df :
                df.index = df["單位名稱"].str.strip()
                df.drop("單位名稱",axis=1,inplace=True)
            elif "指數" in df :
                df.index = df["指數"].str.strip()
                df.drop("指數",axis=1,inplace=True)
            elif "日期" in df :
                df.loc[:,"日期"] = df["日期"].map(lambda x : datefromsplit(x))
                df.index = pd.to_datetime(df["日期"])
                df.drop("日期",axis=1,inplace=True)
            elif "報酬指數" in df :
                df.index = df["報酬指數"].str.strip()
                df.drop("報酬指數",axis=1,inplace=True)
            elif "類型" in df :
                df.index = df["類型"].str.strip()
                df.drop("類型",axis=1,inplace=True)
                
            df.index.name="index"
            
            # 開始添加、變更欄位=======================================
            if "融資融券" in t :
                df.columns = ",".join(df.columns).replace("買進","融券買進").replace("融券買進","融資買進",1).split(",")
                df.columns = ",".join(df.columns).replace("賣出","融券賣出").replace("融券賣出","融資賣出",1).split(",")
                df.columns = ",".join(df.columns).replace("前日餘額","前日融券餘額").replace("前日融券餘額","前日融資餘額",1).split(",")
                df.columns = ",".join(df.columns).replace("今日餘額","今日融券餘額").replace("今日融券餘額","今日融資餘額",1).split(",")
                df.columns = ",".join(df.columns).replace("限額","融券限額").replace("融券限額","融資限額",1).split(",")
            if "買進金額" in df and "賣出金額" in df :
                df.loc[:,"交易總額"] = df["買進金額"] + df["賣出金額"]
            if "外陸資買進股數(不含外資自營商)" in df and "外陸資賣出股數(不含外資自營商)" in df :
                df.loc[:,"外陸資交易總股數(不含外資自營商)"] = df["外陸資買進股數(不含外資自營商)"] + df["外陸資賣出股數(不含外資自營商)"]
            if "外資買進股數" in df and "外資賣出股數" in df :
                df.loc[:,"外資交易總股數"] = df["外資買進股數"] + df["外資賣出股數"]
            if "外陸資買賣超股數(不含外資自營商)" in df and "外資自營商買賣超股數" in df :
                df.loc[:,"外陸資買賣超股數"] = df["外陸資買賣超股數(不含外資自營商)"] + df["外資自營商買賣超股數"]
            if "投信買進股數" in df and "投信賣出股數" in df :
                df.loc[:,"投信交易總股數"] = df["投信買進股數"] + df["投信賣出股數"]
            if "自營商買進股數(自行買賣)" in df and "自營商賣出股數(自行買賣)" in df :
                df.loc[:,"自營商交易總股數(自行買賣)"] = df["自營商買進股數(自行買賣)"] + df["自營商賣出股數(自行買賣)"]
            if "自營商買進股數(避險)" in df and "自營商賣出股數(避險)" in df :
                df.loc[:,"自營商交易總股數(避險)"] = df["自營商買進股數(避險)"] + df["自營商賣出股數(避險)"]
            if "信用額度總量管制" in t:
                df = df.iloc[:,6:]
            if "收盤價" in df and "本益比" in df :
                df.loc[:,"eps"] = df["收盤價"]/df["本益比"]
            if "成交金額" in df and "成交筆數" in df :
                df.loc[:,"成交金額/成交筆數"] = df["成交金額"]/df["成交筆數"]
            if "成交股數" in df and "成交筆數" in df :
                df.loc[:,"成交股數/成交筆數"] = df["成交股數"]/df["成交筆數"]
            if "融資買進" in df and "融資賣出" in df and "現金償還" in df :
                df.loc[:,"融資交易總張數"] = df["融資買進"] + df["融資賣出"] + df["現金償還"]
            if "融券買進" in df and "融券賣出" in df and "現券償還" in df :
                df.loc[:,"融券交易總張數"] = df["融券買進"] + df["融券賣出"] + df["現券償還"]
            if "當日賣出" in df and "當日還券" in df and "當日調整" in df :
                df.loc[:,"借券交易總股數"] = df["當日賣出"] + df["當日還券"] + df["當日調整"]
            if "前日融資餘額" in df and "今日融資餘額" in df :
                df.loc[:,"淨融資"] = df["今日融資餘額"]-df["前日融資餘額"]
            if "前日融券餘額" in df and "今日融券餘額" in df :
                df.loc[:,"淨融券"] = df["今日融券餘額"]-df["前日融券餘額"]
            if "前日餘額" in df and "當日餘額" in df :
                df.loc[:,"淨借券"] = df["當日餘額"]-df["前日餘額"]
            if "融券交易張數" in df and "融券交易張數" in df :
                df.loc[:,"信用交易總張數"] = df["融券交易張數"] + df["融資交易張數"]
            if "信用交易總張數" in df :
                df.loc[:,"信用交易淨額"] =  df["融資交易張數"] - df["融券交易張數"]
            if "今日餘額" in df and "前日餘額" in df :
                df.loc[:,"信用交易淨額"] = df["今日餘額"] - df["前日餘額"]
            if "買進" in df and "賣出" in df and "現金(券)償還" in df :
                df.loc[:,"信用交易總額"] = df["買進"] + df["賣出"] + df["現金(券)償還"]
            if "財報年/季" in df :
                df.loc[:,"財報年/季"] = df["財報年/季"].map(lambda x : datefromsplit(x))
            if "最近一次上市公司申報外資持股異動日期" in df :
                df.loc[:,"最近一次上市公司申報外資持股異動日期"] = df["最近一次上市公司申報外資持股異動日期"].map(lambda x : datefromsplit(x))
            if "最近一次上市公司申報外資及陸資持股異動日期" in df :
                df.loc[:,"最近一次上市公司申報外資及陸資持股異動日期"] = df["最近一次上市公司申報外資及陸資持股異動日期"].map(lambda x : datefromsplit(x))
            if "整體市場" in df and "股票" in df :
                df.loc[:,["整體市場","整體市場漲停"]] = df["整體市場"].str.split("(",expand=True).rename(columns={0:"整體市場",1:"整體市場漲停"})
                df.loc[:,["股票","股票漲停"]] = df["股票"].str.split("(",expand=True).rename(columns={0:"股票",1:"股票漲停"})
                df.replace("\)","",regex=True,inplace=True)
                df = turntofloat(df,col=["整體市場","整體市場漲停","股票","股票漲停"])
            
            if debug == True : continue
        
            try:
    # 開始分為stock 和 market兩種方式來儲存==========================
                if isinstance(df.index,pd.DatetimeIndex) == False :
                    for p in df.index.unique():
                        print("\r{}".format(p),end="")
                        newdf = df.loc[df.index.isin([p]),:]
                        if isinstance(m.findstock(io=p),pd.Series) == True:
                            p=m.findstock(io=p)
                            if p.size!=1:sys.exit()
                            p=p.values[0]
                        savepath = path.join(management.warehouse_path,"product",p,savename.format(s="_"+p))
                        newdf.index = [crawldate]
                        if path.exists(savepath) == True :
                            old = pickleload(savepath)
                            newdf = dfappend(newdf,old)
                        picklesave(savepath,newdf,repl=True)
                elif isinstance(df.index,pd.DatetimeIndex) == True :
                    savepath = path.join(m.warehouse_path,"product",savename.format(s=""))
                    if path.exists(savepath) == True :
                        old = pickleload(savepath)
                        newdf = newdf.append(newdf,old)
                        newdf.drop_duplicates(inplace=True)
                    picklesave(savepath,newdf,repl=True)
    # 存檔完成進行log改寫=========================================
                m.log.loc[crawldate,t] = cf.today
                picklesave(m.olderlog_path,m.log,repl=True)
                print("saving sucessed !!")
                gc.collect()
                # gc.get_threshold()
                # gc.garbage
                # gc.enable()
            except KeyboardInterrupt:
                print("KeyboardInterrupt ... content saving canceled !")
                break
                # sys.exit()
            except Exception as e :
                print("===============")
                print(format_exc())
                print("Unknowned error")
                print(e)
                break
                # sys.exit()

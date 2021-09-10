import configuration as cf
import gc
import sys
from os.path import join,exists
from traceback import format_exc
import pandas as pd
from steventricks.mighty import pickleload, picklesave,  turntofloat, df_append, path_walk, fileload, roctoad, isnumber
from packet import  search_title,rename_dic
from steventricks.db import dbmanager



db=dbmanager(root=cf.cloud_path,db="stocktable")
stocktable=db.alltableget(filename="stocktable")
stocktable.index = stocktable["代號"]+"_"+stocktable["名稱"]

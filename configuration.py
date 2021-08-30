#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
step1==>
step2==>
step3==>
step4==>

result ==> 
test  ==>  

"""

spline=r"="*20+r"{}"+r"="*20
print(spline.format("In the configuration"))
print(__file__,r"...Running __file__")
print(__name__,r"...__main__")
print(r"Initializing....")
import sys
from os import path
# from pathlib import Path
# p=sys.argv[0]
# work_path=os.getcwd()
# work_dir=Path().absolute()
workdir_path=path.dirname(__file__)
# get the current work directory==============================================
dependencies_pkg_source=path.abspath(path.join(workdir_path,".."))
# package_path=os.path.abspath("../..")
# to set the parent dir as dependencies package source========================
sys.path.insert(0,dependencies_pkg_source)
# print(sys.path)
# add to sys.path to get dependencies package=================================

# In[]
import platform
platform=platform.system()
print(platform,"...Running platform")
# to show what the operation system is running
from datetime import date,datetime
today = date.today()
now=datetime.now()

if sys.platform.startswith('win32'):
    cloud_path = path.abspath(workdir_path)
    # Do Windows stuff
elif sys.platform.startswith('darwin'):
    cloud_path = path.abspath(workdir_path)
    # Do MacOS stuff
elif sys.platform.startswith('linux'):
    pass
    # Do Linux stuff
elif sys.platform.startswith("cygwin"):
    pass
else:
    pass

# to add the required source directory to path according to operation system===
print("Done...Initialization")

# In[]
if __name__ == "__main__" : 
    print(r"Running __file__ is : ",__file__)
    print(r"__main__ is : ",__name__)

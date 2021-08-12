import os
from os import path
import sys
import DatabaseInfo
from DatabaseInfo import *
import json
import pickle

def getDBObj():
    cacheOutOfDate = False
    if(os.path.exists("DatabaseInfoCache")):
        if(os.path.exists("tableInfo.json") and os.path.getmtime("tableInfo.json")>os.path.getmtime("DatabaseInfoCache")):
            cacheOutOfDate = True
        if(os.path.exists("dataCategories.json") and os.path.getmtime("dataCategories.json")>os.path.getmtime("DatabaseInfoCache")):
            cacheOutOfDate = True
    else:
        cacheOutOfDate = True

    if(cacheOutOfDate):
        if(os.path.exists("tableInfo.json")):
            with open("tableInfo.json","r") as f:
                try:
                    tableInfo = json.load(f)
                except json.decoder.JSONDecodeError:
                    print("improper file format '{}".format("tableInfo.json"),file=sys.stderr)
                    sys.exit()
            dataCategories = None
            if(os.path.exists("dataCategories.json")):
                with open("dataCategories.json","r") as f:
                    try:
                        dataCategories = json.load(f)
                    except json.decoder.JSONDecodeError:
                        print("improper file format '{}".format("dataCategories.json"),file=sys.stderr)
                        sys.exit()
        else:
            print("database info file does not exist '{}'".format("tableInfo.json"),file=sys.stderr)
            sys.exit()
        dbInfo = DatabaseInfo(tableInfo,dataCategories)
        with open("DatabaseInfoCache","wb") as f:
            pickle.dump(dbInfo,f,protocol = pickle.HIGHEST_PROTOCOL)

    if(not cacheOutOfDate):
        with open("DatabaseInfoCache","rb") as f:
            f.seek(0)
            dbInfo = pickle.load(f)



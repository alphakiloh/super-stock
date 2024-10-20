
import re
import os
import sys
import pandas as pd
import urllib.request 
import requests 
import yfinance as yf
from requests import Session
import numpy as np
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
import datetime

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

GDRIVE_FOLDER_ID = "15IHdGgeH4YeZH19fD8Ni7eYXvoiHBSsJ"
CREDENTIAL_FOLDER_PATH = "../auth" # relative to location of THIS file

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)


def log(m, t=True):
    
    if t:
        ts = str(datetime.datetime.now())
    else:
        ts = ""

    dir_path = os.path.dirname(os.path.realpath(__file__))

    f = open(dir_path + "/" + "exec.log", "a")
    print(ts + m)
    f.write(ts + m + "\n")
    f.close()


def get_auth_filepath():

    # search in CREDENTIAL_FOLDER_PATH for JSON credential file
    dir_path = os.path.dirname(os.path.realpath(__file__))
    path = dir_path + "/" + CREDENTIAL_FOLDER_PATH

    for filename in os.listdir(path):
        if re.match(".*\\.json", filename):
            return path + "/" + filename  
        else:
            raise Exception("JSON credential file not found!")    

def main():

    # get tickers from www.sec.gov
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        "User-Agent" : "Foo Bar foo@bar.com",
        "Accept-Encoding" : "gzip, deflate",
        "Host" : "www.sec.gov"
    }

    req = urllib.request.Request(url, headers=headers)

    with urllib.request.urlopen(req) as response:
        df = pd.read_json(response, compression="gzip")

    # transpose the dataframe
    df = df.T


    # DEBUG : limit to first n tickers
    #df.drop(df.tail(len(df.index) - 20).index, inplace=True)

    #print(df)

    #exit()



    columns = {
        # First three already pulled from SEC.gov
        "cik_str"       : "text",
        "ticker"        : "text",
        "title"         : "text",
        # rest need to be added
        "price"         : "float",
        "inc"           : "year",
        "cap"           : "int",
        "float"         : "int",
        "insiders"      : "percent",
        "institutions"  : "percent",
        "earnings1"     : "text",
        "earnings2"     : "text",
        "sector"        : "text",
        "industry"      : "text",
        "hi52"          : "float",
        "lo52"          : "float",
        "strength"      : "float",
        "pTotalMarket"  : "float",
        "pSector"       : "float",
        "pIndustry"     : "float",
        "sma50"         : "float",
        "sma150"        : "float",
        "sma200"        : "float",
        "eEq"            : "int",
        "eps1"          : "float",
        "eps2"          : "float",
        "eps3"          : "float",
        "eps4"          : "float",
        "eps5"          : "float",
        "eps6"          : "float",
        "eRq"            : "int",
        "rev1"          : "int",
        "rev2"          : "int",
        "rev3"          : "int",
        "rev4"          : "int",
        "rev5"          : "int",
        "rev6"          : "int",
        #"grossMargin"   : "percent",
        #"netMargin"     : "percent",
        #"opMargin"      : "percent",
        "website"       : "text",
        "profile"       : "text"
    }

    field_names = list(columns.keys())
    formats = list(columns.values())


    for c in field_names[3:]:
        df[c] = ""

    # G:My Drive\Finance\Screener\   
    
    ts = datetime.datetime.now()
    wb_name = "screener_" + str(ts)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # https://developers.google.com/workspace/guides/create-credentials <--service account: download JSON file, share folder with account email
    gauth_path = get_auth_filepath()
        
    creds = Credentials.from_service_account_file(gauth_path, scopes=scopes)
    gs = gspread.authorize(creds)
     
    wb = gs.create(title= wb_name, folder_id=GDRIVE_FOLDER_ID)
    wb.sheet1.update_title("Data")

    gs_format = []

    c1 = "A"
    c2 = ""
    x = 1
    for f in formats:
        col = c1 + c2
        match f:
            case "int":
                d = {
                    "range" : f"{col}:{col}",                
                    "format" : {
                        "numberFormat" : {
                            "type" : "NUMBER",
                            "pattern" : "#,##0"
                        }
                    }
                }

            case "float":
                d = {
                    "range" : f"{col}:{col}",                
                    "format" : {
                        "numberFormat" : {
                            "type" : "NUMBER",
                            "pattern" : "#,##0.00;(#,##0.00)"
                        }
                    }
                }

            case "percent":
                d = {
                    "range" : f"{col}:{col}",                
                    "format" : {
                        "numberFormat" : {
                            "type" : "NUMBER",
                            "pattern" : "0%"
                        }
                    }
                }
            case "text":
                d = {
                    "range" : f"{col}:{col}",                
                    "format" : {
                        "textFormat" : {
                            "bold" : False,
                        }
                    }
                }
            case "date":
                d = {
                    "range" : f"{col}:{col}",                
                    "format" : {
                        "numberFormat" : {
                            "type" : "DATE",
                            "pattern" : "yyyy-mm-dd"
                        }
                    }
                }
        gs_format.append(d)
        if c1 == "Z":
            c1 = "A"
            c2 = "A"
        elif len(c2) == 0 :
            c1 = chr(ord(c1)+1)
        else:
            c2 = chr(ord(c2)+1)




    #df_err = pd.DataFrame

    #s2 = wb.add_worksheet("Errors", 0, 0)

    # to track failing tickers
    bad_tickers = []


    for i, row in df.iterrows():
    #for i, row in df.iloc[:5].iterrows():

        ts = datetime.datetime.now()
        
        log(
            " ........................................... " +
            "{:05}".format(i) + 
            " of " + 
            "{:05}".format(len(df.index) - 1) +
            " : " +  row["ticker"]   
        ) 
        
        ticker = yf.Ticker(row["ticker"], session=session)
        hist = ticker.history(period="1y")
        hist = hist.iloc[::-1]
        price = df.loc[i, "price"] = hist.iloc[0]["Close"]

        log(" : summary & historical data retrieved")

        if len(hist.index) < 252:
            log(" : *********** not enough historical price data!!!!! ***********\n")
            bad_tickers.append(row["ticker"])
            df.loc[i, "ticker"] = ""
            continue

        sma50 = df.loc[i, "sma50"] = sum(hist["Close"][:50].tolist()) / 50
        sma150 = df.loc[i, "sma150"] = sum(hist["Close"][:150].tolist()) / 150
        sma200 = df.loc[i, "sma200"] = sum(hist["Close"][:200].tolist()) / 200

        c63 = hist.iloc[62]["Close"]
        c126 = hist.iloc[125]["Close"]
        c189 = hist.iloc[188]["Close"]
        c252 = hist.iloc[251]["Close"]

        rs = df.loc[i, "strength"] = 2*price/c63 + price/c126 + price/c189 + price/c252

        hi52 = df.loc[i, "hi52"] = hist["High"].max()
        lo52 = df.loc[i, "lo52"] = hist["Low"].min()
        


        df.loc[i, "profile"] = biz_summary = ticker.info.get("longBusinessSummary")

        m = re.search('founded in ([0-9]{4})', biz_summary)
        if not m:
            m = re.search('incorporated in ([0-9]{4})', biz_summary)
        if m:    
            df.loc[i, "inc"] = m.group(1)

        df.loc[i, "cap"]           = ticker.info.get("marketCap")
        df.loc[i, "float"]         = ticker.info.get("floatShares")
        df.loc[i, "insiders"]      = ticker.info.get("heldPercentInsiders")
        df.loc[i, "institutions"]  = ticker.info.get("heldPercentInstitutions")
        df.loc[i, "sector"]        = ticker.info.get("sector")
        df.loc[i, "industry"]      = ticker.info.get("industry")
        df.loc[i, "website"]       = ticker.info.get("website")


        if ticker.calendar:
            df.loc[i, "earnings1"] = ticker.calendar["Earnings Date"][0]
            if len(ticker.calendar["Earnings Date"]) > 1:
                df.loc[i, "earnings2"] = ticker.calendar["Earnings Date"][1]
        
        log(" : retrieving quarterly financials")

        q_stmt = ticker.quarterly_incomestmt

        if not q_stmt.empty:
            eps = q_stmt.loc["Basic EPS"].tolist()
            rev = q_stmt.loc["Total Revenue"].tolist()


            log(" : recording EPS data")

            for q in range(len(eps)):
                col_name = "eps" + str(q+1)
                df.loc[i, col_name] = eps[q]


            log(" : recording Revenue data")

            for q in range(len(rev)):
                col_name = "rev" + str(q+1)
                df.loc[i, col_name]  = rev[q]


            log(" : calculating number of growth quarters")

            # count the number of escalating quarters of revenue and earnings
            eEq = eRq = 0 
            q = 1
            while q < len(eps) and eps[q-1] > eps[q]:
                eEq += 1
                q += 1
            log(f" : processed q={q} quarters of Earnings")

            while q < len(rev) and rev[q-1] > rev[q]:
                eRq += 1  
                q +=1
            log(f" : processed q={q} quarters of Revenue")

            df.loc[i, "eEq"] = eEq
            df.loc[i, "eRq"] = eRq
        else:
            log(" : *********** could not retrieve quarterly financial statements!!!!! ***********\n")

            # set ticker to NULL string, so we can filter it out out later
            bad_tickers.append(row["ticker"])
            df.loc[i, "ticker"] = ""


        loop_dt = datetime.datetime.now() - ts
        loop_seconds = loop_dt.total_seconds()
        hrs_remaining = (loop_seconds * (len(df.index) - i)) / 3600

        log(f"|| Completed in {loop_seconds:.2f} seconds. Estimating {hrs_remaining:.2f} hours to complete scraping.\n", False)


    # drop all tickers that failed and had their sybmols set to NULL string
    df.drop(df[df["ticker"] == ""].index, inplace=True)
    log(" ........................:\n\nthe following tickers failed:\n")
    for i in range(len(bad_tickers)):
        log(bad_tickers[i], False)


    log(" : calculating relative strength\n")

    #pTotalMarket
    df["pTotalMarket"] = df["strength"].rank(method="max", pct=True)

    #pSector
    df["pSector"] = df.groupby(["sector"])["strength"].rank(method="max", pct=True)

    #pIndustry
    df["pIndustry"] = df.groupby(["industry"])["strength"].rank(method="max", pct=True)


    log(" : writing to gsheet\n")


    # write dataframe to g-sheet
    s1 = wb.sheet1

    set_with_dataframe(s1, df)

    s1.batch_format(gs_format)

    gs_header_format = [
        {
            "range" : "1:1",                
            "format" : {
               "textFormat" : {
                   "bold" : True,
                   "horizontalAlignment" : "CENTER"
               }
            }
        }
    ]
    
    s1.format("1:1", {"textFormat": {"bold": True}})
    s1.format("1:1", {"horizontalAlignment": "CENTER"})
    s1.freeze(rows=1,cols=2)
    s1.hide_columns(start=0, end=1)

    


    #print(df)

main()




    


   









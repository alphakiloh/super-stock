
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

GDRIVE_FOLDER_ID = "15IHdGgeH4YeZH19fD8Ni7eYXvoiHBSsJ" # G:My Drive\Finance\Screener\  
CREDENTIAL_FOLDER_PATH = "../auth" # relative to location of THIS file

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(1, Duration.SECOND*1)),  # max 1 requests per 2 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

def log(m, t=True):
    
    # add timestamp if T
    if t:
        ts = str(datetime.datetime.now())
    else:
        ts = ""

    dir_path = os.path.dirname(os.path.realpath(__file__))

    # log to CLI
    print(ts + m)

    # duplicate log to file
    f = open(dir_path + "/" + "exec.log", "a")
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

    log(" : retieving tickers from SEC.gov")

    req = urllib.request.Request(url, headers=headers)

    log(" : initializing our dataframe with tickers")

    with urllib.request.urlopen(req) as response:
        df = pd.read_json(response, compression="gzip")

    # transpose the dataframe
    df = df.T

    #print(df)

    #print(df.loc[df["ticker"] == "PUBGY"])
    #print(df.loc[df["ticker"] == "AAPL"])

    # DEBUG : limit to first n tickers
    #df.drop(df.tail(len(df.index) - 20).index, inplace=True)

    # DEBUG : limit to small number of tickers, including trouble tickers
    #df = df[df["ticker"].isin(["PUBGY","AAPL","NVDA","GOOG","NFLX"])]
    #print(df)

    #exit()

    columns = {
        # first three fields already defined by pull from SEC.gov
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
        "earnings1"     : "date",
        "earnings2"     : "date",
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
        "eEq"           : "int",
        "eps1"          : "float",
        "eps2"          : "float",
        "eps3"          : "float",
        "eps4"          : "float",
        "eps5"          : "float",
        "eps6"          : "float",
        "eRq"           : "int",
        "rev1"          : "int",
        "rev2"          : "int",
        "rev3"          : "int",
        "rev4"          : "int",
        "rev5"          : "int",
        "rev6"          : "int",
        #"grossMargin"   : "percent",
        #"netMargin"     : "percent",
        #"opMargin"      : "percent",
        "ttc"           : "text",     # trend template criteria
        "website"       : "text",
        "profile"       : "text"
    }

    field_names = list(columns.keys())
    formats = list(columns.values())

    log(" : buildinging out the columns of our dataframe")

    # append columns and set values to NULL string
    for c in field_names[3:]:
        df[c] = ""

    log(" : establishing connections to g-sheet")
     
    # open connection to g-sheet
    ts = datetime.datetime.now()
    wb_name = "screener_" + str(ts)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    log(" : getting g-auth credentials from JSON file in ../auth")

    # https://developers.google.com/workspace/guides/create-credentials <--service account: download JSON file, share folder with account email
    gauth_path = get_auth_filepath()
        
    log(" : establishing connections to g-sheet")

    creds = Credentials.from_service_account_file(gauth_path, scopes=scopes)
    gs = gspread.authorize(creds)
     
    log(" : initializing our g-sheet")

    wb = gs.create(title= wb_name, folder_id=GDRIVE_FOLDER_ID)
    wb.sheet1.update_title("Data")

    log(" : building the gspread format dictionary list")

    # build list of dictionaries to feed to gspread to fromat our columns
    # assume we do not need more than 52 columns (i.e. column AZ)
    gs_format = []
    c1 = "A"
    c2 = ""

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

    #s2 = wb.add_worksheet("Errors", 0, 0)

    # to track failing tickers
    bad_tickers = []

    log("\n\n : start scraping\n\n")

    num_tickers = len(df.index)

    for i, row in df.iterrows():
    #for i, row in df.iloc[:5].iterrows():

        # we'll calculate time delta at end of the loop to estimate running time left
        ts = datetime.datetime.now()
        
        log(
            " ........................................... " +
            "{:05}".format(i) + 
            " of " + 
            "{:05}".format(num_tickers - 1) +
            " : " +  row["ticker"]   
        ) 
        
        ticker = yf.Ticker(row["ticker"], session=session)

        log(" : retrieving quarterly financials")

        q_stmt = ticker.quarterly_incomestmt

        # if no quarterly statement, the ticker is not a company with filings
        # and reading empty financials causes crash
        if not q_stmt.empty:

            try:
                eps = q_stmt.loc["Basic EPS"].tolist()
            except:
                log(" : ***** MISSING DATA ***** | quarterly finanicals wihout EPS |")
                bad_tickers.append(row["ticker"])
                log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n")
                df.loc[i, "ticker"] = ""
                continue

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
            log(" : ***** MISSING DATA ***** | could not retrieve quarterly financial statements |")

            # set ticker to NULL string, so we can filter it out out later
            bad_tickers.append(row["ticker"])
            log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n")
            df.loc[i, "ticker"] = ""
            continue

        log(" : retrieving historical price data")

        hist = ticker.history(period="1y")
        
        # we want a full year of price history
        # setting ticker to NULL string means it gets filtered/dropped later
        if len(hist.index) < 252:
            log(" : ***** MISSING DATA ***** | not enough historical price data |")
            bad_tickers.append(row["ticker"])
            log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n")
            df.loc[i, "ticker"] = ""
            continue

        # sort descending    
        hist = hist.iloc[::-1]

        price = df.loc[i, "price"] = hist.iloc[0]["Close"]
        hi52 = df.loc[i, "hi52"] = hist["High"].max()
        lo52 = df.loc[i, "lo52"] = hist["Low"].min()

        log(" : calculating moving averages")

        # calculate simple moving averages
        sma50 = df.loc[i, "sma50"] = sum(hist["Close"][:50].tolist()) / 50
        sma150 = df.loc[i, "sma150"] = sum(hist["Close"][:150].tolist()) / 150
        sma200 = df.loc[i, "sma200"] = sum(hist["Close"][:200].tolist()) / 200

        log(" : calculating relative strength")

        # relative strength (most recent quarter weighted double) will be compared 
        # against total market, sector, and industry to calcate percentile rankings
        # in order to approximate IBD score
        c63 = hist.iloc[62]["Close"]
        c126 = hist.iloc[125]["Close"]
        c189 = hist.iloc[188]["Close"]
        c252 = hist.iloc[251]["Close"]
        rs = df.loc[i, "strength"] = 2*price/c63 + price/c126 + price/c189 + price/c252

        log(" : checking Trend Template Criteria")

        # check to see if company meets the Trend Template Criteria (can't check inside loop: relative-strength >= .7)
        if price > sma50 > sma150 > sma200 and price >= 1.3 * lo52 and price >= .75 * hi52:
            df.loc[i, "ttc"] = "Pass"
        else:
            df.loc[i, "ttc"] = "Fail"

        log(" : extracting date of incorporation from company blurb")

        # get the founding/incorporated year from company summary blurb
        df.loc[i, "profile"] = biz_summary = ticker.info.get("longBusinessSummary")
        m = re.search('founded in ([0-9]{4})', biz_summary)
        if not m:
            m = re.search('incorporated in ([0-9]{4})', biz_summary)
        if m:    
            df.loc[i, "inc"] = m.group(1)

        log(" : recording cap, float, and other data from INFO object")

        # get() prevents failures on (missing) key errors
        df.loc[i, "cap"]           = ticker.info.get("marketCap")
        df.loc[i, "float"]         = ticker.info.get("floatShares")
        df.loc[i, "insiders"]      = ticker.info.get("heldPercentInsiders")
        df.loc[i, "institutions"]  = ticker.info.get("heldPercentInstitutions")
        df.loc[i, "sector"]        = ticker.info.get("sector")
        df.loc[i, "industry"]      = ticker.info.get("industry")
        df.loc[i, "website"]       = ticker.info.get("website")

        log(" : extracting Earnings Date(s) from calendar data")

        # reading NULL calendar causes crash, and even if not NULL, sometimes the Earnings Date key is not there
        if ticker.calendar:
            earnings_date = ticker.calendar.get("Earnings Date")
            for dt_i in range(len(earnings_date)):
                df.loc[i, f"earnings{dt_i + 1}"] = earnings_date[dt_i]

        # calculate how long a loop iteration lasts and estimate scraping time left
        loop_dt = datetime.datetime.now() - ts
        loop_seconds = loop_dt.total_seconds()
        hrs_remaining = (loop_seconds * (num_tickers - i)) / 3600

        log(f" : {row["ticker"]} completed in {loop_seconds:.2f} seconds - estimating {hrs_remaining:.2f} hours to complete scraping of remaining {num_tickers - i} tickers\n")

    # drop all tickers that failed and had their symbols set to NULL string
    df.drop(df[df["ticker"] == ""].index, inplace=True)
    
    if(len(bad_tickers) > 0):
        log(" : ***** | the following tickers failed |\n")
        for i in range(len(bad_tickers)):
            log(bad_tickers[i], False)

    log("\n : calculating relative strength")

    # get percentile rank against total market, sector, and industry
    df["pTotalMarket"] = df["strength"].rank(method="max", pct=True)
    df["pSector"] = df.groupby(["sector"])["strength"].rank(method="max", pct=True)
    df["pIndustry"] = df.groupby(["industry"])["strength"].rank(method="max", pct=True)

    log(" : writing to gsheet")

    # write dataframe to g-sheet
    s1 = wb.sheet1

    set_with_dataframe(s1, df)

    log(" : formatting the gsheet")

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

    log(" : all done!!")    

    #print(df)

main()




    


   









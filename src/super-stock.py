
import re
import itertools
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

 
CREDENTIAL_FOLDER_PATH = "../auth" # relative to location of THIS file
LOG_FOLDER_PATH = "../log" # relative to location of THIS file

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(1, Duration.SECOND*1)),  # max 1 requests per 1 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

def log(message, filename="", timestamp=True, cli=True):
    
    # add timestamp 
    if timestamp:
        ts = str(datetime.datetime.now())
    else:
        ts = ""

    # log to CLI
    if cli:
        print(ts + message)

    # log to file
    if filename != "":
        dir_path = os.path.dirname(os.path.realpath(__file__))
        f = open(dir_path + "/" + LOG_FOLDER_PATH  + "/" + filename, "a")
        f.write(ts + message + "\n")
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

def main(): # takes one argument: the Google Drive folder ID as a string

    # create matching log and gsheet filenames 
    ts = str(datetime.datetime.now()).replace(" ", "_")
    script_name  = re.search(r"([^\/]+?)(\.py)$", sys.argv[0]).group(1)
    gsheet_filename =  script_name + "_" + ts 
    log_filename = script_name + "_" + ts + ".log"

    # get tickers from www.sec.gov
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        "User-Agent" : "Foo Bar foo@bar.com",
        "Accept-Encoding" : "gzip, deflate",
        "Host" : "www.sec.gov"
    }

    log(" : retieving tickers from SEC.gov", log_filename)

    req = urllib.request.Request(url, headers=headers)

    log(" : initializing our dataframe with tickers", log_filename)

    with urllib.request.urlopen(req) as response:
        df = pd.read_json(response, compression="gzip")

    # transpose the dataframe
    df = df.T

    # DEBUG : limit to first n tickers
    #df.drop(df.tail(len(df.index) - 20).index, inplace=True)

    # DEBUG : limit to small number of tickers, including trouble tickers
    #df = df[df["ticker"].isin(["PHYS","AAPL","NVDA","GOOG","NFLX"])]
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
        "eps7"          : "float",
        "eps8"          : "float",
        "eps9"          : "float",
        "eEqRa"         : "int",
        "eps1ra"        : "float",
        "eps2ra"        : "float",
        "eps3ra"        : "float",
        "eps4ra"        : "float",
        "eps5ra"        : "float",
        "eps6ra"        : "float",
        "eps7ra"        : "float",
        "eps8ra"        : "float",
        "eps9ra"        : "float",
        "eRq"           : "int",
        "rev1"          : "int",
        "rev2"          : "int",
        "rev3"          : "int",
        "rev4"          : "int",
        "rev5"          : "int",
        "rev6"          : "int",
        "rev7"          : "int",
        "rev8"          : "int",
        "rev9"          : "int",
        "eNm"           : "int",
        "netMargin1"    : "percent",
        "netMargin2"    : "percent",
        "netMargin3"    : "percent",
        "netMargin4"    : "percent",
        "netMargin5"    : "percent",
        "netMargin6"    : "percent",
        "netMargin7"    : "percent",
        "netMargin8"    : "percent",
        "netMargin9"    : "percent",
        "ttc"           : "text",     # trend template criteria
        "website"       : "text",
        "profile"       : "text"
    }

    field_names = list(columns.keys())


    log(" : building out the columns of our dataframe", log_filename)

    # append columns and set values to NULL string
    for c in field_names[3:]:
        df[c] = ""

    #s2 = wb.add_worksheet("Bad Tickers", 0, 0) - TO_DO : collect information (nature of failure, etc) about bad tickers on sheet2 of the gsheet

    # to track failing tickers
    bad_tickers = []

    log(" : start scraping\n\n", log_filename)

    num_tickers = len(df.index)

    # df's i(ndex) key is not necessarily (1,2,3,...) incremental (like when we start with a filtered frame for debugging), use loop_count for estimating execution time
    loop_count = 0

    for i, row in df.iterrows():
    #for i, row in df.iloc[:5].iterrows():

        # we'll calculate time delta at end of the loop to estimate running time left
        ts = datetime.datetime.now()
        
        loop_count += 1
        
        log(
            " ........................................... " +
            "{:05}".format(loop_count) + 
            " of " + 
            "{:05}".format(num_tickers) +
            " : " +  row["ticker"],
            log_filename  
        ) 
        
        ticker = yf.Ticker(row["ticker"], session=session)

        log(" : retrieving quarterly financials", log_filename)

        q_stmt = ticker.quarterly_incomestmt

        # if no quarterly statement, the ticker is not a company with filings
        # and reading empty financials causes crash
        if not q_stmt.empty:

            q_stmt_col_names = list(q_stmt.columns)

            try:
                eps = q_stmt.loc["Basic EPS"].tolist()
            except:
                log(" : ***** MISSING DATA ***** | quarterly finanicals without EPS |", log_filename)
                bad_tickers.append(row["ticker"])
                log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n", log_filename)
                df.loc[i, "ticker"] = ""
                continue

            try:
                rev = q_stmt.loc["Total Revenue"].tolist()
            except:
                log(" : ***** MISSING DATA ***** | quarterly finanicals without Revenue |", log_filename)
                bad_tickers.append(row["ticker"])
                log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n", log_filename)
                df.loc[i, "ticker"] = ""
                continue

            try:
                netIncome = q_stmt.loc["Net Income"].tolist()
            except:
                log(" : ***** MISSING DATA ***** | quarterly finanicals without Net Income |", log_filename)
                bad_tickers.append(row["ticker"])
                log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n", log_filename)
                df.loc[i, "ticker"] = ""
                continue

            log(" : recording EPS data", log_filename)

            for q in range(len(eps)):
                
                # if EPS is missing try to calculate it from net income and outstanding shares, either from current or previous quarter            
                if np.isnan(eps[q]):
                
                    num_shares = q_stmt.loc["Basic Average Shares", q_stmt_col_names [q]]
                    if np.isnan(num_shares) and q < len(eps) - 1:
                        num_shares = q_stmt.loc["Basic Average Shares", q_stmt_col_names [q + 1]]

                    net_income = q_stmt.loc["Net Income", q_stmt_col_names[q]]
                    if np.isnan(net_income) and q < len(eps) - 1:
                        net_income = q_stmt.loc["Net Income", q_stmt_col_names[q + 1]]
                
                    if not np.isnan(num_shares) and not np.isnan(num_shares):
                        eps[q] = net_income / num_shares
                
                col_name = "eps" + str(q+1)
                df.loc[i, col_name] = eps[q] 

            for q in range(len(eps)):    
                # 2q rolling avg, replace negative earnings with 0 when averaging
                col_name = "eps" + str(q+1) + "ra"

                if q < len(eps) - 1 and not np.isnan(eps[q + 1]):
                    df.loc[i, col_name] = (max(eps[q], 0) + max(eps[q + 1], 0)) / 2
                else:
                    df.loc[i, col_name] = eps[q]

            log(" : recording revenue and margin", log_filename)

            for q in range(len(rev)):
                col_name = "rev" + str(q+1)
                df.loc[i, col_name]  = rev[q]

                col_name = "netMargin" + str(q+1)
                df.loc[i, col_name] = netIncome[q] / rev[q]

            log(" : calculating number of growth quarters", log_filename)

            # count the number of escalating EPS quarters 
            eEq = 0 
            q = 1
            while q < len(eps) and eps[q-1] >= eps[q]:
                eEq += 1
                q += 1
            df.loc[i, "eEq"] = eEq

            # count the number of escalating EPS quarters (smoothed to 2q rolling avg)
            eEqRa = 0 
            q = 1
            while q < len(eps) and df.loc[i, f"eps{q}ra"] >= df.loc[i, f"eps{q + 1}ra"]:
                eEqRa += 1
                q += 1
            df.loc[i, "eEqRa"] = eEqRa

            # count the number of escalating Revenue quarters
            eRq = 0 
            q = 1
            while q < len(rev) and rev[q-1] >= rev[q]:
                eRq += 1  
                q +=1
            df.loc[i, "eRq"] = eRq

            # count the number of escalating Margin quarters
            eNm = 0 
            q = 1
            while q < len(eps) and df.loc[i, f"netMargin{q}"] >= df.loc[i, f"netMargin{q + 1}"]:
                eNm += 1
                q += 1
            df.loc[i, "eNm"] = eNm

        else:
            log(" : ***** MISSING DATA ***** | could not retrieve quarterly financial statements |", log_filename)

            # set ticker to NULL string, so we can filter it out out later
            bad_tickers.append(row["ticker"])
            log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n", log_filename)
            df.loc[i, "ticker"] = ""
            continue

        log(" : retrieving historical price data", log_filename)

        hist = ticker.history(period="1y")
        
        # we want a full year of price history
        # setting ticker to NULL string means it gets filtered/dropped later
        if len(hist.index) < 251:
            log(" : ***** MISSING DATA ***** | not enough historical price data |", log_filename)
            bad_tickers.append(row["ticker"])
            log(f" : {df.loc[i, "ticker"]} will be dropped from dataframe when scraping completes\n", log_filename)
            df.loc[i, "ticker"] = ""
            continue

        # sort descending : TO_DO: try sort_values() - the history request section takes 2 seconds on avg instead of expected 1 sec to execute the request
        hist = hist.iloc[::-1]

        price = df.loc[i, "price"] = hist.iloc[0]["Close"]
        hi52 = df.loc[i, "hi52"] = hist["High"].max()
        lo52 = df.loc[i, "lo52"] = hist["Low"].min()

        log(" : calculating moving averages", log_filename)

        # calculate simple moving averages
        sma50 = df.loc[i, "sma50"] = sum(hist["Close"][:50].tolist()) / 50
        sma150 = df.loc[i, "sma150"] = sum(hist["Close"][:150].tolist()) / 150
        sma200 = df.loc[i, "sma200"] = sum(hist["Close"][:200].tolist()) / 200

        log(" : calculating relative strength", log_filename)

        # relative strength (most recent quarter weighted double) will be compared 
        # against total market, sector, and industry to calcate percentile rankings
        # in order to approximate IBD score
        c63 = hist.iloc[62]["Close"]
        c126 = hist.iloc[125]["Close"]
        c189 = hist.iloc[188]["Close"]
        c252 = hist.iloc[len(hist.index)-1]["Close"] # full year could be 251 before closing bell
        rs = df.loc[i, "strength"] = 2*price/c63 + price/c126 + price/c189 + price/c252

        log(" : checking Trend Template Criteria", log_filename)

        # check to see if company meets the Trend Template Criteria (can't check inside loop: relative-strength >= .7)
        if price > sma50 > sma150 > sma200 and price >= 1.3 * lo52 and price >= .75 * hi52:
            df.loc[i, "ttc"] = "Pass"
        else:
            df.loc[i, "ttc"] = "Fail"

        log(" : extracting date of incorporation from company blurb", log_filename)

        # get the founding/incorporated year from company summary blurb
        df.loc[i, "profile"] = biz_summary = ticker.info.get("longBusinessSummary")
        if biz_summary:
            m = re.search('founded in ([0-9]{4})', biz_summary)
            if not m:
                m = re.search('incorporated in ([0-9]{4})', biz_summary)
            if m:    
                df.loc[i, "inc"] = m.group(1)

        log(" : recording cap, float, and other data from INFO object", log_filename)

        # get() prevents failures on (missing) key errors
        df.loc[i, "cap"]           = ticker.info.get("marketCap")
        df.loc[i, "float"]         = ticker.info.get("floatShares")
        df.loc[i, "insiders"]      = ticker.info.get("heldPercentInsiders")
        df.loc[i, "institutions"]  = ticker.info.get("heldPercentInstitutions")
        df.loc[i, "sector"]        = ticker.info.get("sector")
        df.loc[i, "industry"]      = ticker.info.get("industry")
        df.loc[i, "website"]       = ticker.info.get("website")

        log(" : extracting Earnings Date(s) from calendar data", log_filename)

        # reading NULL calendar causes crash, and even if not NULL, sometimes the Earnings Date key is not there
        if ticker.calendar:
            earnings_date = ticker.calendar.get("Earnings Date")
            for dt_i in range(len(earnings_date)):
                df.loc[i, f"earnings{dt_i + 1}"] = earnings_date[dt_i]

        # calculate how long a loop iteration lasts and estimate scraping time remaining
        loop_dt = datetime.datetime.now() - ts
        loop_seconds = loop_dt.total_seconds()
        hrs_remaining = (loop_seconds * (num_tickers - loop_count)) / 3600

        log(f" : {row["ticker"]} completed in {loop_seconds:.2f} seconds - estimating {hrs_remaining:.2f} hours to complete scraping of remaining {num_tickers - loop_count} tickers\n", log_filename)

    # drop all tickers that failed and had their symbols set to NULL string
    df.drop(df[df["ticker"] == ""].index, inplace=True)
    
    if(len(bad_tickers) > 0):
        log(" : ***** | the following tickers failed |\n", log_filename)
        for i in range(len(bad_tickers)):
            log(bad_tickers[i], log_filename, False)
            log("\n", log_filename, False)

    log(" : calculating relative strength percentiles", log_filename)

    # get percentile rank against total market, sector, and industry
    df["pTotalMarket"] = df["strength"].rank(method="max", pct=True)
    df["pSector"] = df.groupby(["sector"])["strength"].rank(method="max", pct=True)
    df["pIndustry"] = df.groupby(["industry"])["strength"].rank(method="max", pct=True)

    log(" : getting g-auth credentials from JSON file in ../auth", log_filename)

    # https://developers.google.com/workspace/guides/create-credentials <--service account: download JSON file, share folder with account email
    gauth_path = get_auth_filepath()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(gauth_path, scopes=scopes)

    log(" : establishing connections to g-sheet", log_filename)
      
    gs = gspread.authorize(creds)
     
    log(" : initializing our g-sheet", log_filename)

    wb = gs.create(title= gsheet_filename, folder_id=sys.argv[1])
    wb.sheet1.update_title("Data")

    log(" : building the gspread format dictionary list", log_filename)

    # build list of dictionaries to feed to gspread to format our columns
    formats = list(columns.values())
    gs_format = []
    c1 = "A"
    c2 = ""

    # A,B,C ...ZZ
    ascii_upper = list(map(chr, range(ord("A"), ord("Z")+1)))
    column_index_names = list(map("".join, itertools.product(ascii_upper, ascii_upper)))
    column_index_names = ascii_upper + column_index_names

    for i, f in enumerate(formats):
        col = column_index_names[i]
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

    log(" : writing to gsheet", log_filename)

    # write dataframe to g-sheet
    s1 = wb.sheet1

    set_with_dataframe(s1, df)

    log(" : formatting the gsheet", log_filename)

    s1.batch_format(gs_format)
   
    s1.format("1:1", {"textFormat": {"bold": True}})
    s1.format("1:1", {"horizontalAlignment": "CENTER"})
    s1.freeze(rows=1,cols=3)
    s1.hide_columns(start=0, end=1)

    log("\n\n all done !!\n", log_filename, False)    

    #print(df)

main()




    


   









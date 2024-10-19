
import re
import pandas as pd
import urllib.request 
import requests 
import yfinance as yf
from requests import Session
from numpy import nan
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
import datetime

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

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

    columns = [
        "price",
        "inc",
        "cap",
        "float",
        "insiders",
        "institutions",
        "earnings1",
        "earnings2",
        "sector",
        "industry",
        "hi52",
        "lo52",
        "rs",
        "sma50",
        "sma150",
        "sma200",
        "eq",
        "eps1",
        "eps2",
        "eps3",
        "eps4",
        "eps5",
        "eps6"
        "rev1",
        "rev2",
        "rev3",
        "rev4",
        "rev5",
        "rev6",
        "grossMargin",
        "netMargin",
        "opMargin",
        "website",
        "profile"
    ]

    for c in columns:
        df[c] = ""

 
    # for index, row in df.iterrows():
    for i, row in df.iloc[:1].iterrows():
        
        ticker = yf.Ticker(row["ticker"], session=session)
        hist = ticker.history(period="1y")
        hist = hist.iloc[::-1]
        price = df.loc[i, "price"] = hist.iloc[0]["Close"]

        if len(hist.index) < 252:
            continue

        sma50 = df.loc[i, "sma50"] = sum(hist["Close"][:50].tolist()) / 50
        sma150 = df.loc[i, "sma150"] = sum(hist["Close"][:150].tolist()) / 150
        sma200 = df.loc[i, "sma200"] = sum(hist["Close"][:200].tolist()) / 200

        c63 = hist.iloc[62]["Close"]
        c126 = hist.iloc[125]["Close"]
        c189 = hist.iloc[188]["Close"]
        c252 = hist.iloc[251]["Close"]

        rs = df.loc[i, "rs"] = 2*price/c63 + price/c126 + price/c189 + price/c252

        hi52 = df.loc[i, "hi52"] = hist["High"].max()
        lo52 = df.loc[i, "lo52"] = hist["Low"].min()
        


        df.loc[i, "profile"] = biz_summary = ticker.info["longBusinessSummary"]

        m = re.search('founded in ([0-9]{4})', biz_summary)
        if not m:
            m = re.search('incorporated in ([0-9]{4})', biz_summary)
        if m:    
            #print(m.group(1))
            df.loc[i, "inc"] = m.group(1)

        #print(ticker.info)

        df.loc[i, "cap"]           = ticker.info["marketCap"]
        df.loc[i, "float"]         = ticker.info["floatShares"]
        df.loc[i, "insiders"]      = ticker.info["heldPercentInsiders"]
        df.loc[i, "institutions"]  = ticker.info["heldPercentInstitutions"]
        df.loc[i, "sector"]        = ticker.info["sector"]
        df.loc[i, "industry"]      = ticker.info["industry"]
        df.loc[i, "website"]       = ticker.info["website"]

        df.loc[i, "earnings1"] = ticker.calendar["Earnings Date"][0]
        if len(ticker.calendar["Earnings Date"]) > 1:
            df.loc[i, "earnings2"] = ticker.calendar["Earnings Date"][1]

        q_stmt = ticker.quarterly_incomestmt

        eps = q_stmt.loc['Basic EPS'].tolist()
        rev = q_stmt.loc['Total Revenue'].tolist()

        for q in range(len(eps)):
            col_name = 'eps' + str(q+1)
            df.loc[i, col_name] = eps[q]

        for q in range(len(rev)):
            col_name = 'rev' + str(q+1)
            df.loc[i, col_name]  = rev[q]

     


    ct = datetime.datetime.now()
    wb_name = "screener_" + str(ct)



    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]


    # https://developers.google.com/workspace/guides/create-credentials <--service account: download JSON file, share folder with account email
    gauth_path = "../auth/screener-420319-cee3c8c66be8.json"

    creds = Credentials.from_service_account_file(gauth_path, scopes=scopes)
    gc = gspread.authorize(creds)
    

    # G:My Drive\Finance\Screener\
    folder_id = "15IHdGgeH4YeZH19fD8Ni7eYXvoiHBSsJ"
    
    wb = gc.create(title= wb_name, folder_id=folder_id)
    s1 = wb.sheet1

    set_with_dataframe(s1, df)
    #s1.update([df.columns.values.tolist()] + df.fillna("NaN").values.tolist())

    print(df)

main()


    


   









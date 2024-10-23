import gspread
import itertools
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# gsheet_write(str, str, str, list, dataFrame)
def gsheet_write(folder_id, title, gauth_path, formats, df):

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(gauth_path, scopes=scopes)
    gs = gspread.authorize(creds)
    wb = gs.create(title = title, folder_id = folder_id)
    s1 = wb.sheet1
    s1.update_title("Data")

    # build list of dictionaries to feed to gspread to format our columns
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

    set_with_dataframe(s1, df)
    s1.batch_format(gs_format)
    s1.format("1:1", {"textFormat": {"bold": True}})
    s1.format("1:1", {"horizontalAlignment": "CENTER"})
    s1.freeze(rows=1,cols=3)
    s1.hide_columns(start=0, end=1)
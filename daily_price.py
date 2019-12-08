# -*- coding: utf-8 -*-
"""
Created on Sun Jul  7 23:04:10 2019

@author: ABMRazin
"""

import pandas as pd
import urllib.request, urllib.parse, urllib.error
from sqlalchemy import create_engine
import pymysql
import time
import datetime as dt

conn = pymysql.connect(host = "localhost", user = "root", passwd = "13Tallabagh")
cur = conn.cursor()

# Note that existing database is being used which has been created for fundamental data.
# For this repo to work, go through edgar_scraper repo, you atleast need to download and 
# run the set.py and wikitable_scraper.py files for daily_price.py file to work 

# Using existing database
cur.execute("use financial_database1")

engine = create_engine("mysql+pymysql://root: 13Tallabagh@localhost: 3306/financial_database1")
dbConnection = engine.connect()

def metadata():
    
    # Get metadata from existing database, id and ticker is used
    
    df = pd.read_sql_table("symbol", con = engine)
    id_lst = df["id"].tolist()
    ticker_lst = df["ticker"].tolist()
    
    return id_lst, ticker_lst


def daily_price(IDX, COMPANIES_TO_RUN, endDate, token):
    
    
    # The function calls metadata and get the id and ticker of the companies for which daily price 
    # data will be accumilated. Each ticker is run on a loop, price data is fetched from tiingo and 
    # exported into existing database
    
    
    data = metadata()
    ids = data[0]
    tickers = data[1]
    
    price_unavailable = []

    for ticker in tickers[IDX:IDX + COMPANIES_TO_RUN]:
        
        # Get the last date from the database, if it exists for which price is saved in the database
        try:
            sqlQuery = "select date from daily_price where ticker = '{0}' order by date DESC limit 1".format(ticker)
            dbDate = pd.read_sql(sqlQuery, dbConnection)
            
            startDate = dbDate["date"].tolist()[0]
            startDate = startDate + dt.timedelta(days = 1)
            startDate = dt.datetime.strftime(startDate, "%Y-%m-%d")
        
        # If no date exist in db, start from "2000-01-03"
        except:
            startDate = "2000-01-03"
        
        url = "https://api.tiingo.com/tiingo/daily/"+ticker+"/prices"
        params = {"startDate": startDate, "endDate": endDate, "token": token}
        url_parts = list(urllib.parse.urlparse(url))
        query = dict(urllib.parse.parse_qsl(url_parts[4]))
        query.update(params)
        url_parts[4] = urllib.parse.urlencode(query)
        urlname = urllib.parse.urlunparse(url_parts)
        print (urlname)
        
        try:
            df_price = pd.read_json(urlname)
            
            if df_price.empty:
                print ("Price table for ticker {0} is upto date".format(ticker))
                continue
            
            df_price["symbol_id"] = [ids[IDX] for i in range(len(df_price))]
            df_price["ticker"] = [ticker for i in range(len(df_price))]
            df_price["data_vendor"] = ["Tiingo" for i in range(len(df_price))]
            
            df_price.to_sql("daily_price", con = engine, if_exists = "append")
            
            print ("Data entered into Database: {0}".format(ticker))
            time.sleep(5)
            print ("Restarting......")
        
        except:
            price_unavailable.append(ticker)
            print ("Ticker unavailable: {0}".format(ticker))
            pass
        
        IDX += 1
    
    print (price_unavailable)
    
    return df_price

price = daily_price(IDX = 0, COMPANIES_TO_RUN = 5, endDate = "2019-12-07", token = "fa5cf2883a3fa0920b0a66ae5357fd47ce9d3cb8")
        

# You can add your own list of securities and get prices without the database support


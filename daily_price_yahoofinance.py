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
from yahoofinancials import YahooFinancials

conn = pymysql.connect(host = "localhost", user = "root", passwd = "13Tallabagh")
cur = conn.cursor()

# Note that existing database is being used which is created for fundamental data.
# For this repo to work, go through edgar_scraper repo, you atleast need to download and 
# run the set.py and wikitable_scraper.py files for daily_price.py file to work 

# Using existing database
cur.execute("use financial_database1")

engine = create_engine("mysql+pymysql://root:13Tallabagh@localhost:3306/financial_database1")
#dbConnection = engine.connect()
#engine.connect()

def metadata():
    
    # Get metadata from existing database, id and ticker is used
    
    df = pd.read_sql_table("symbol", con = engine)
    id_lst = df["id"].tolist()
    ticker_lst = df["ticker"].tolist()
    
    return id_lst, ticker_lst

#x = metadata()
#x[1][71:72]
#
#ticker = "BRK-B"
#yahoo_financials = YahooFinancials(ticker)
#
#data = yahoo_financials.get_historical_price_data(start_date = "2020-01-01", end_date = "2020-08-26", time_interval = "daily")
#data
#x = data[ticker]["prices"]


# Update S&P500 Price Database (Source: Yahoo Finance)
def daily_price(IDX, COMPANIES_TO_RUN, END_DATE, db):
    
    metadata_lst = metadata()
    
    id_lst = metadata_lst[0]
    ticker_lst = metadata_lst[1]
    ticker_lst = ticker_lst[IDX:COMPANIES_TO_RUN]
    
    price_unavailable = []
    
    for ticker in ticker_lst:
        
        if db == "use":
#            print ("db_use")
            sqlQuery = "select max(date) from daily_price where ticker = '{0}'".format(ticker)
            db_date = pd.read_sql_query(sqlQuery, con=engine)
            START_DATE = db_date["max(date)"][0]
            
            try:
                START_DATE = START_DATE.strftime("%Y-%m-%d")
            except:
                pass
            
            if START_DATE == None:
                START_DATE = "2000-01-01"

        elif db == "nonuse":
#            print ("db_nonuse")
            ## Change START_DATE to reflect last date in db 
            START_DATE = "2021-01-05"
        
        try:
            ticker = ticker.replace(".", "-")
            yahoo_financials = YahooFinancials(ticker)
            data = yahoo_financials.get_historical_price_data(start_date=START_DATE,
                                                              end_date=END_DATE,
                                                              time_interval='daily')
            
            try:
                price_data = data[ticker]["prices"]
            except:
                IDX +=1
                continue
            
            date_lst = []
            open_lst = []
            high_lst = []
            low_lst = []
            adjclose_lst = []
            close_lst = []
            volume_lst = []
            
            for prices in price_data:
                date = prices["formatted_date"]
                date_lst.append(date)
                
                open_price = prices["open"]
                open_lst.append(open_price)
                
                high_price = prices["high"]
                high_lst.append(high_price)
                
                low_price = prices["low"]
                low_lst.append(low_price)
                
                adjclose_price = prices["adjclose"]
                adjclose_lst.append(adjclose_price)
                
                close_price = prices["close"]
                close_lst.append(close_price)
                
                volume = prices["volume"]
                volume_lst.append(volume)
            
            df_price = pd.DataFrame()
            
            df_price["open"] = open_lst
            df_price["high"] = high_lst
            df_price["low"] = low_lst
            df_price["adjClose"] = adjclose_lst
            df_price["close"] = close_lst
            df_price["volume"] = volume_lst
            
            df_price["date"] = date_lst
            
            df_price["ticker"] = [ticker for i in range(len(df_price))]
            df_price["ticker"] = df_price["ticker"].apply(lambda x: x.replace("-", "."))
            df_price["symbol_id"] = [id_lst[IDX] for i in range(len(df_price))]    
            IDX += 1
            
            df_price["data_vendor"] = ["Yahoo Finance" for i in range(len(df_price))]
            
            df_price = df_price.sort_values(by=["date"], ascending=True)
            
            cols_to_keep = ["symbol_id", "ticker", "open", "high", "low", "adjClose", "close", "volume", "date", "data_vendor"]
            df_price = df_price[cols_to_keep]
            
            if db == "use":
                df_price = df_price[df_price["date"] > START_DATE]
            elif db == "nonuse":
                pass
            
            df_price = df_price.drop_duplicates(subset = ["date"])
            df_price.to_sql("daily_price", con = engine, if_exists = "append")
            
            print ("Data entered into Database: {0}".format(ticker))            
        
        except:
            price_unavailable.append(ticker)
            IDX += 1
            print ("Ticker unavailable: {0}".format(ticker))
            pass
    
    print (price_unavailable)

    return df_price, price_unavailable

x = daily_price(0, 506, "2021-01-06", db = "nonuse")

#def daily_price(IDX, COMPANIES_TO_RUN, endDate, token):
#    
#    
#    # The function calls metadata and get the id and ticker of the companies for which daily price 
#    # data will be accumilated. Each ticker is run on a loop, price data is fetched from tiingo and 
#    # exported into existing database
#    
#    
#    data = metadata()
#    ids = data[0]
#    tickers = data[1]
#    
#    price_unavailable = []
#
#    for ticker in tickers[IDX:IDX + COMPANIES_TO_RUN]:
#                
#        # Get the last date from the database, if it exists for which price is saved in the database
#        try:
#            sqlQuery = "select date from daily_price where ticker = '{0}' order by date DESC limit 1".format(ticker)
#            dbDate = pd.read_sql(sqlQuery, con = engine)
#            
#            startDate = dbDate["date"].tolist()[0]
#            startDate = startDate + dt.timedelta(days = 1)
#            startDate = dt.datetime.strftime(startDate, "%Y-%m-%d")
#        
#        # If no date exist in db, start from "2000-01-03"
#        except:
#            startDate = "2000-01-03"
#        
#        url = "https://api.tiingo.com/tiingo/daily/"+ticker+"/prices"
#        params = {"startDate": startDate, "endDate": endDate, "token": token}
#        url_parts = list(urllib.parse.urlparse(url))
#        query = dict(urllib.parse.parse_qsl(url_parts[4]))
#        query.update(params)
#        url_parts[4] = urllib.parse.urlencode(query)
#        urlname = urllib.parse.urlunparse(url_parts)
#        print (urlname)
#        
#        try:
#            df_price = pd.read_json(urlname)
#            
#            if df_price.empty:
#                print ("Price table for ticker {0} is upto date".format(ticker))
#                IDX += 1
#                continue
#            
#            df_price["symbol_id"] = [ids[IDX] for i in range(len(df_price))]
#            df_price["ticker"] = [ticker for i in range(len(df_price))]
#            df_price["data_vendor"] = ["Tiingo" for i in range(len(df_price))]
#            
#            df_price.to_sql("daily_price", con = engine, if_exists = "append")
#            
#            print ("Data entered into Database: {0}".format(ticker))
#            time.sleep(5)
#            print ("Restarting......")
#        
#        except:
#            price_unavailable.append(ticker)
#            print ("Ticker unavailable: {0}".format(ticker))
#            pass
#
#        IDX += 1
#        
#    print (price_unavailable)
#    
#    return df_price
#
#price = daily_price(IDX = 0, COMPANIES_TO_RUN = 505, endDate = "2020-05-15", token = "fa5cf2883a3fa0920b0a66ae5357fd47ce9d3cb8")
        

# You can add your own list of securities and get prices without the database support


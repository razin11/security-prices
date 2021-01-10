# security-prices

ABOUT THE PROJECT

The project is an attempt to collect price data of securities and save it an a mysql database. I have two price scrapers on file (daily_price.py for tiingo and daily_price_yahoofinance.py for Yahoo Finance). Note that this project is highly complementary to the edgar-scraper project (https://github.com/razin11/edgar-scraper) and would recommend visiting the site before running the daily_price.py file. However, this repo could would work stand-alone as well if you run the setup file and follow the instructions in the requirements.txt file. The end product is a mysql database in your local server thtat contains price data of sp500 companies. 

HOW THE MODEL WORKS

First, when the setup.py file is run, a wikipedia page is scraped which contains data of all sp500 companies. That data is exported to a mysql database. The daily_price.py file gets the ticker and id data from the database and requests price data for each company from tiingo or yahoo finance, whichever scraper is being used, which delivers the data in json format, which is then converted into a dataframe and exported to the database. Measures have been taken to prevent duplication of data. 

Since database only contains metadata of sp500 companies, prices of these companies will only be collected. To extend beyond sp500 just add new companies to the symbol table in the database. 

The main function daily_price in daily_price.py file, takes initial id, no. of companies to run at a time, the last date of prices to collect and tiingo token as arguments. For the program to work you need to obtain a personal token from tiingo and also download mysql.

RELATED PROJECTS

Visit the above link to get an idea of the final end product this repo is built to accomplish. 

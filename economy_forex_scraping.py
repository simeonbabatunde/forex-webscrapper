#! /usr/bin/env python
'''
#File-Name:        economy_forex_scraping.py.py            
# Date:            2016-11-14                         
# Author:          Simeon Babatunde  ... 
# Purpose:         Scrap FOREX data from ABOKIFX website into VGG EDH and BI's S3 bucket via flume
# Data Used:       www.abokifx.com
#				   http://www.exchangerates.org.uk
#Script Location   EDH node 1
#
# Packages Used:   BeautifulSoup
# History:
#                  v20161111    Initial implementation of the scraper
#                  v20161114    Script revamped to scrape the ticker content in addition to the main site
#                  Script does not takes an argument
#                  Run on the command line as
#                  ./economy_forex_scraping.py
#                  
# 
'''


#import the library used to query a website
import urllib2
#import the Beautiful soup functions to parse the data returned from the website
from bs4 import BeautifulSoup
#import other guys
from datetime import date, timedelta, datetime
from decimal import Decimal
import pandas as pd

#initialize a variable to for most current data
LASTEST = 2
LASTEST_1 = 1
ABOKI_FX_DATE_FORMAT = "%d/%m/%y"
ABOKI_FX_DATE_FORMAT2 = "%d/%m/%Y"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
TODAY = date.today().strftime(DATE_FORMAT)
CURRENT_DATE_TIME = datetime.now().strftime(DATE_FORMAT)
YESTERDAY = (date.today() - timedelta(1)).strftime(DATE_FORMAT)



#these are some of the most useful functions for this operation
def _to_date(date_string, aboki_fromat):
    new_date = datetime.strptime(date_string, aboki_fromat).strftime(DATE_FORMAT)
    return new_date


def _split_rate(rate):
    buy_n_sell = rate.replace("*", "").split(" / ")
    return buy_n_sell

def _to_currency(value):
    return float(value + ".00")

def _generate_rate(today_rates, dateformat):
    date = _to_date(today_rates[0].text, dateformat)
    dollar_buy = _split_rate(today_rates[1].string)[0]
    dollar_sell = _split_rate(today_rates[1].string)[1]
    pound_buy = _split_rate(today_rates[2].string)[0]
    pound_sell = _split_rate(today_rates[2].string)[1]
    euro_buy = _split_rate(today_rates[3].string)[0]
    euro_sell = _split_rate(today_rates[3].string)[1]
    exchange_data = {"eur_sell": _to_currency(euro_sell),"eur_buy": _to_currency(euro_buy), 
                     "gbp_sell": _to_currency(pound_sell),"gbp_buy": _to_currency(pound_buy),
                     "usd_sell": _to_currency(dollar_sell),"usd_buy": _to_currency(dollar_buy), 
                    "event_date": CURRENT_DATE_TIME, "abokifx_date": date}
    return exchange_data


def _generate_rate_african(today_rates, keys):
    ghs_buy = _split_rate(today_rates[1].string)[0]
    ghs_sell = _split_rate(today_rates[1].string)[1]
    xof_buy = _split_rate(today_rates[2].string)[0]
    xof_sell = _split_rate(today_rates[2].string)[1]
    xaf_buy = _split_rate(today_rates[3].string)[0]
    xaf_sell = _split_rate(today_rates[3].string)[1]
    values = (_to_currency(ghs_buy), _to_currency(ghs_sell), _to_currency(xof_buy), _to_currency(xof_sell), _to_currency(xaf_buy), _to_currency(xaf_sell))
    exchange_data = dict(zip(keys,values))
    return exchange_data


def _generate_rate_cbn_mgram_bdc(today_rates, keys):
    usd = today_rates[1].string
    gbp = today_rates[2].string
    eur = today_rates[3].string
    values = (usd, gbp, eur)
    exchange_data = dict(zip(keys, float(values))
    return exchange_data

def get_ticker_rate(ticker_content):
	tickers = []
	values = []
	for i in ticker_content:
	    try:
	        ticker = i.find(("a", {"target": "_top"})).text.replace("/", "_").lower().replace("ngn_","")+"_ngn"
	        value = 1/float(i.find('span').text.encode('ascii','ignore'))
	        tickers.append(ticker)
	        values.append(round(value, 2))
	    except:
	        continue
        
	return dict(zip(tickers, values))

#Check if exchange rates are today's else mail analyst"""
def get_rate(today_rates):
     return _generate_rate(today_rates, ABOKI_FX_DATE_FORMAT)

def get_rate_afri(afri_rates):
	keys = ("ghs_buy", "ghs_sell", "xof_buy",  "xof_sell", "xaf_buy", "xaf_sell")
	return _generate_rate_african(afri_rates, keys)

def get_rate_bdc(bdc_rates):
	keys = ("bdc_usd_buy", "bdc_usd_sell", "bdc_gbp_buy",  "bdc_gbp_sell", "bdc_eur_buy", "bdc_eur_sell")
	return _generate_rate_african(bdc_rates, keys)
    
def get_rate_cbn(cbn_today):
	keys = ('cbn_usd', 'cbn_gbp', 'cbn_eur')
	return _generate_rate_cbn_mgram_bdc(cbn_today, keys)

def get_rate_mgram(mgram_today):
	keys = ('mgram_usd', 'mgram_gbp', 'mgram_eur')
	return _generate_rate_cbn_mgram_bdc(mgram_today, keys)

def get_rate_wunioin(wunion_today):
	keys = ('wunioin_usd', 'wunion_gbp', 'wunion_eur')
	return _generate_rate_cbn_mgram_bdc(wunion_today, keys)

def main():
	#specify the url to the forex data to be scraped from abokifx.com
	front_page = "http://abokifx.com/"
	bdc_page = "http://abokifx.com/bdcs/"
	ex_rates_uk_url = "http://www.exchangerates.org.uk/widget/ER-LRTICKER.php?w=490&s=1&mc=NGN&mbg=262626&bs=no&bc=FFFF21&f=verdana&fs=10px&fc=F2F20A&lc=FCE408&lhc=FE9A00&vc=FE9A00&vcu=00E800&vcd=FF0000&"


	#Query the website and return the html to the variable 'page'
	aboki_home = urllib2.urlopen(front_page)
	bdc_home = urllib2.urlopen(bdc_page)
	ex_rates_raw = urllib2.urlopen(ex_rates_uk_url)

	#Parse the html in the 'aboki_home' variable, and store it in Beautiful Soup format
	soup_home = BeautifulSoup(aboki_home, "lxml")
	bdc_soup_home = BeautifulSoup(bdc_home, "lxml")
	ex_rates_ticker = BeautifulSoup(ex_rates_raw, "lxml")

	tabless = soup_home.find_all('table')
	bdc_tabless = bdc_soup_home.find_all('table')
	ticker_content = ex_rates_ticker.find_all("span", {"class": "quote"})


	#extract the parallel market table
	daily_table = tabless[2]
	african_table = tabless[3]
	bdc_table = bdc_tabless[2]
	cbn_table = tabless[8]
	mgram_table = tabless[7]
	wunion_table = tabless[6]

	#to extract the parallel market table, use the class name for the table
	rates_table = daily_table.findAll('tr')
	afri_table = african_table.findAll('tr')
	bdc_tbl = bdc_table.findAll('tr')
	cbn_rate_tbl = cbn_table.find_all('tr')
	mgram_rate_tbl = mgram_table.find_all('tr')
	wunion_rate_tbl = wunion_table.find_all('tr')

	today_rates = rates_table[LASTEST].find_all('td')
	afri_rates = afri_table[LASTEST].find_all('td')
	bdc_rates = bdc_tbl[LASTEST].find_all('td')
	cbn_today = cbn_rate_tbl[LASTEST_1].find_all('td')
	mgram_today = mgram_rate_tbl[LASTEST_1].find_all('td')
	wunion_today = wunion_rate_tbl[LASTEST_1].find_all('td')

	#collate all the dictionaries in a single array
	l = [get_rate(today_rates), get_rate_afri(afri_rates), get_rate_bdc(bdc_rates), get_rate_cbn(cbn_today), get_rate_wunioin(wunion_today), get_rate_mgram(mgram_today), get_ticker_rate(ticker_content)]
	#initialize a dict
	t = {}
	#merge the individual dicts to a unified one
	for i in l:
	    t.update(i)
	    
	print(t)


if __name__ == '__main__':
	main()








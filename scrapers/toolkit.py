from bs4 import BeautifulSoup
import requests
from functools import lru_cache
import pandas as pd
from selenium import webdriver
driver = webdriver.Chrome()


@lru_cache(maxsize=None)
def scrape(html):
    page = requests.get(html)
    html_code = BeautifulSoup(page.content, "lxml")
    return page, html_code


@lru_cache(maxsize=None)
def html_pandas(html):
    table_list = pd.read_html(html)
    return table_list


def javascript_scraper(html):
    driver.get(html)
    return driver

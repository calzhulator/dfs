from bs4 import BeautifulSoup
import requests
from functools import lru_cache


@lru_cache(maxsize=None)
def scrape(html):
    page = requests.get(html)
    html_code = BeautifulSoup(page.content, "lxml")
    return page, html_code
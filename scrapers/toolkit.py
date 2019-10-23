from bs4 import BeautifulSoup
import requests


def scrape(html):
    page = requests.get(html)
    html_code = BeautifulSoup(page.content, "lxml")
    return page, html_code
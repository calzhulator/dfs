import scrapers.toolkit as tools
import pandas as pd
import time
from bs4 import BeautifulSoup


def get_contest_stats(date):
    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    if date < '2016-09-11':
        return None, None
    html = "https://rotogrinders.com/resultsdb/site/draftkings/date/{date}/sport/nfl/".format(date=date)
    driver = tools.javascript_scraper(html)
    time.sleep(5)
    arrows = driver.find_elements_by_class_name('ant-select-arrow')
    slate_drop = arrows[1]
    slate_drop.click()
    time.sleep(5)
    dropdowns = driver.find_elements_by_class_name('ant-select-dropdown-menu-item')
    all_data_contests = {}
    all_data_ownerships = {}
    for drop in dropdowns:
        drop_name = drop.text
        if drop_name == 'No Slates Available':
            return None, None
        drop.click()
        time.sleep(5)
        elements = driver.find_elements_by_class_name('ant-tabs-tab')
        for el in elements:
            if el.text in ['Contests', 'Ownership']:
                el.click()
                time.sleep(5)
        html_code = BeautifulSoup(driver.page_source, "lxml")
        # contest table
        if len(html_code.find_all('table')) == 4:
            contests_table = html_code.find_all('table')[2]
            rows = contests_table.find_all('tr')
            contest_df = {}
            for i in range(len(rows)):
                if i == 0:
                    headers = [x.get_text() for x in rows[i].find_all('span', {'class': 'ant-table-header-column'})]
                else:
                    test_list = [x.get_text().replace('$', '').replace(',', '') if x.get_text() != 'Contest'
                                 else x.find('a')['href'] for x in rows[i].find_all('td')]
                    contest_df[i] = [x if x != '' else None for x in test_list]
            contest_df = pd.DataFrame(contest_df, index=headers).T
            float_cols = ['Prize Pool', 'Buy In', 'Top Prize', 'Max Entries', 'Entries', 'Cash Line', 'Winning Score']
            type_dict = {stat: float for stat in float_cols}
            contest_df = contest_df.astype(type_dict)
            all_data_contests[drop_name] = contest_df
            # ownership table
            ownership_table = html_code.find_all('table')[3]
            rows = ownership_table.find_all('tr')
            ownership_df = {}
            for i in range(len(rows)):
                if i == 0:
                    headers = [x.get_text().replace('$', '').replace(',', '')
                               for x in rows[i].find_all('span', {'class': 'ant-table-header-column'})]
                else:
                    td_rows = rows[i].find_all('td')
                    output = []
                    for x in td_rows:
                        td_text = x.get_text().split(' (')[0]
                        if '%' in td_text:
                            output.append(float(td_text.replace('%', '')) / 100.0)
                        else:
                            test_text = td_text.replace('/FLEX', '')
                            if test_text == '':
                                output.append(None)
                            else:
                                output.append(td_text.replace('/FLEX', ''))
                    ownership_df[i] = output + [None] * (len(headers) - len(output))
            ownership_df = pd.DataFrame(ownership_df, index=headers).T
            float_cols = ['Fpts']
            type_dict = {stat: float for stat in float_cols}
            ownership_df = ownership_df.astype(type_dict)
            all_data_ownerships[drop_name] = ownership_df
        slate_drop.click()
        time.sleep(5)
    return all_data_contests, all_data_ownerships

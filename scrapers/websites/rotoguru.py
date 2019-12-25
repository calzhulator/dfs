import scrapers.toolkit as tools
import pandas as pd

site_mappings = {'fanduel': 'fd',
                 'yahoo': 'yh',
                 'draftkings': 'dk'}


def get_platform_data(platform, year, week):
    url = "http://rotoguru1.com/cgi-bin/fyday.pl?week={w}&year={y}&game={p}&scsv=1".format(w=week, y=year,
                                                                                           p=site_mappings[platform])
    page, html_code = tools.scrape(url)
    list_strings = html_code.find_all('pre')[0].get_text().split('\n')[:-1]
    if len(list_strings) == 1:
        return None
    load_df = pd.DataFrame([x.split(';') for x in list_strings[1:]])
    load_df.columns = list_strings[0].split(';')
    load_df = load_df.rename({'{p} salary'.format(p=site_mappings[platform].upper()): 'salary',
                              '{p} points'.format(p=site_mappings[platform].upper()): 'points'}, axis=1)
    load_df['salary'] = load_df['salary'].apply(lambda x: None if x in ('', '0') else x)
    load_df['points'] = load_df['points'].apply(lambda x: None if x == '' else x)
    load_df['Pos'] = load_df['Pos'].str.replace('PK', 'K').replace('Def', 'DST')
    type_dict = {'Week': int, 'Year': int, 'points': float, 'salary': float}
    load_df = load_df.astype(type_dict)
    return load_df.dropna()
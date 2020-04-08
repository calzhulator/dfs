import requests
import pandas as pd
import scrapers.toolkit as tools
import time


main_site = "https://www.fantasypros.com/"
default_fields = ['PLAYER', 'SOURCE', 'TEAM']
positions = {'qb': ['pass_att', 'pass_cmp', 'pass_yds', 'pass_td', 'pass_int', 'rush_att',
                    'rush_yds', 'rush_td', 'fumbles_lost', 'FPTS_HALF'],
             'rb': ['rush_att', 'rush_yds', 'rush_td', 'rec', 'rec_yds', 'rec_td', 'fumbles_lost', 'FPTS_HALF'],
             'wr': ['rec', 'rec_yds', 'rec_td', 'rush_att', 'rush_yds', 'rush_td', 'fumbles_lost', 'FPTS_HALF'],
             'te': ['rec', 'rec_yds', 'rec_td', 'fumbles_lost', 'FPTS_HALF'],
             'k': ['FG', 'FGA', 'XPTS', 'FPTS_HALF'],
             'dst': ['SACK', 'INT', 'FR', 'FF', 'TD', 'SAFETY', 'PA', 'YDSAG', 'FPTS_HALF']}
renames = {'def_ff': 'FF', 'def_fr': 'FR', 'def_int': 'INT', 'def_pa': 'PA', 'def_retd': 'RETTD', 'def_sack': 'SACK',
           'def_safety': 'SAFETY', 'def_td': 'TD', 'def_tyda': 'YDSAG', 'fg': 'FG', 'fga': 'FGA', 'fpid': 'SOURCE',
           'fumbles': 'fumbles_lost', 'name': 'PLAYER', 'pass_ints': 'pass_int', 'pass_tds': 'pass_td',
           'points': 'FPTS_STD', 'points_half': 'FPTS_HALF', 'points_ppr': 'FPTS_FULL', 'position_id': 'position',
           'rec_rec': 'rec', 'rec_tds': 'rec_td', 'rush_tds': 'rush_td', 'team_id': 'TEAM', 'xpt': 'XPTS'}
api_key = 'kofMTivg5R2iAkaoJypST6wtL4vC2Lws70Yfm40S'


def get_projections_old(year, week):
    if year < 2012:
        return None
    concat_df = pd.DataFrame()
    for pos in positions.keys():
        url = main_site + 'nfl/projections/{pos}.php?year={year}&week={week}&scoring=HALF'.format(pos=pos,
                                                                                                  year=year, week=week)
        page, html_code = tools.scrape(url)
        body_code = html_code.find_all('tbody')[0].find_all('tr')
        if len(body_code) > 1:
            body_data = {}
            count = 0
            for trs in body_code:
                data_strip = trs.find_all('td')
                row_data = {}
                second_count = 0
                for ds in data_strip:
                    if ds.get('class')[0] == 'player-label':
                        info = ds.find('a')
                        row_data[second_count] = info.get_text()
                        second_count = second_count + 1
                        row_data[second_count] = ds.find_all('a')[1].get('class')[1].replace('fp-id-', '')
                        second_count = second_count + 1
                        row_data[second_count] = ds.get_text().strip().split(' ')[-1] if pos != 'DST' else info.get_text()
                    else:
                        row_data[second_count] = ds.get_text()
                    second_count = second_count + 1
                if row_data[0] != '':
                    body_data[count] = row_data
                    count = count + 1
            body_data = pd.DataFrame(body_data).T
            body_data.columns = default_fields + positions[pos]
            type_dict = {stat: float for stat in positions[pos]}
            body_data = body_data.astype(type_dict)
            body_data['position'] = pos.upper()
            body_data['year'] = year
            body_data['week'] = week
            if pos != 'DST':
                body_data = body_data[body_data['FPTS_HALF'] >= 5.0]
            if 'rec' in body_data.columns:
                body_data['FPTS_FULL'] = body_data['FPTS_HALF'] + .5 * body_data['rec'].fillna(0.0)
                body_data['FPTS_STD'] = body_data['FPTS_HALF'] - .5 * body_data['rec'].fillna(0.0)
            else:
                body_data['FPTS_FULL'] = body_data['FPTS_HALF']
                body_data['FPTS_STD'] = body_data['FPTS_HALF']
            concat_df = concat_df.append(body_data, ignore_index=True, sort=True)
        else:
            return None
    return concat_df


def get_projections(year, week):
    if year < 2012:
        return None
    time.sleep(3)
    page = requests.get('https://api.fantasypros.com/public/v2/json/nfl/{y}/projections'.format(y=year),
                        params={'week': week, 'positions': ':'.join(positions.keys()).upper()},
                        headers={'x-api-key': api_key})
    if page.status_code == 429:
        return None
    json_obj = page.json()
    altered_list = []
    for play in json_obj['players']:
        dict_play = {}
        for k, v in play.items():
            if k == 'stats':
                for k2, v2 in play[k].items():
                    dict_play[k2] = v2
            else:
                dict_play[k] = v
        altered_list.append(dict_play)
    altered_df = pd.DataFrame(altered_list)
    altered_df = altered_df.rename(renames, axis=1)
    altered_df['year'] = year
    altered_df['week'] = week
    return altered_df[(altered_df['position'] == 'DST') | (altered_df['FPTS_HALF'] > 5.0)]
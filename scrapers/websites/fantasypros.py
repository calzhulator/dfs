import scrapers.toolkit as tools
import pandas as pd


main_site = "https://www.fantasypros.com/"
default_fields = ['PLAYER', 'SOURCE', 'TEAM']
positions = {'qb': ['pass_att', 'pass_cmp', 'pass_yds', 'pass_td', 'pass_int', 'rush_att',
                    'rush_yds', 'rush_td', 'fumbles_lost', 'FPTS_HALF'],
             'rb': ['rush_att', 'rush_yds', 'rush_td', 'rec', 'rec_yds', 'rec_td', 'fumbles_lost', 'FPTS_HALF'],
             'wr': ['rec', 'rec_yds', 'rec_td', 'rush_att', 'rush_yds', 'rush_td', 'fumbles_lost', 'FPTS_HALF'],
             'te': ['rec', 'rec_yds', 'rec_td', 'fumbles_lost', 'FPTS_HALF'],
             'k': ['FG', 'FGA', 'XPTS', 'FPTS_HALF'],
             'dst': ['SACK', 'INT', 'FR', 'FF', 'TD', 'SAFETY', 'PA', 'YDSAG', 'FPTS_HALF']}


def get_projections(year, week):
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
            concat_df = concat_df.append(body_data, ignore_index=True, sort=True)
        else:
            return None
    return concat_df
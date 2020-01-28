import scrapers.toolkit as tools
import pandas as pd


main_site = "https://www.fftoday.com/"
default_fields = ['PLAYER', 'SOURCE', 'TEAM', 'OPP']
positions = {('qb', 10): ['pass_cmp', 'pass_att', 'pass_yds', 'pass_td', 'pass_int', 'rush_att',
                    'rush_yds', 'rush_td', 'FPTS_HALF'],
             ('rb', 20): ['rush_att', 'rush_yds', 'rush_td', 'rec', 'rec_yds', 'rec_td', 'FPTS_HALF'],
             ('wr', 30): ['rec', 'rec_yds', 'rec_td', 'FPTS_HALF'],
             ('te', 40): ['rec', 'rec_yds', 'rec_td', 'FPTS_HALF'],
             ('k', 80): ['FG', 'FG_MISS', 'XPTS', 'XPTS_MISS', 'FPTS_HALF']}


def get_projections(year, week):
    concat_df = pd.DataFrame()
    for (pos, num_key) in positions.keys():
        finished = False
        count_page = 0
        all_data = {}
        data_counter = 0
        while not finished:
            url = main_site + 'rankings/playerwkproj.php?Season={y}&GameWeek={w}&PosID={p}&' \
                              'LeagueID=193033&order_by=FFPts&sort_order=DESC&cur_page={c}'.format(
                                y=year, w=week, p=num_key, c=count_page)
            page, html_code = tools.scrape(url)
            trs = html_code.find_all('tr')
            found = False
            for tr_count in range(len(trs[:-1])):
                tr = trs[tr_count]
                if found:
                    list_row = []
                    data_fields = tr.find_all('td')
                    for x in data_fields[1:]:
                        list_row.append(x.get_text().strip())
                        check_href = x.find('a')
                        if check_href is not None:
                            list_row.append(check_href['href'].split('/')[3])
                    all_data[data_counter] = list_row
                    data_counter = data_counter + 1
                if ('class' in tr.attrs.keys()) and (tr.attrs['class'] == ['tableclmhdr']):
                    if tr_count == (len(trs) - 2):
                        finished = True
                    else:
                        found = True
            count_page = count_page + 1
        all_data = pd.DataFrame(all_data).T
        all_data.columns = default_fields + positions[(pos, num_key)]
        type_dict = {stat: float for stat in positions[(pos, num_key)]}
        all_data = all_data.astype(type_dict)
        all_data['position'] = pos.upper()
        all_data['year'] = year
        all_data['week'] = week
        concat_df = concat_df.append(all_data, ignore_index=True, sort=True)
    return concat_df
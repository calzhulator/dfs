import scrapers.toolkit as tools
import scrapers.websites.google as gl
import data.explorer as exp
import pandas as pd
import data.loader as load

main_site = "https://www.pro-football-reference.com/"


def find_player_page(name, filters=None, allow_missing=False):
    name = name.replace('.', '').replace('\'', '').replace(',', '')
    search_url = main_site + "search/search.fcgi?search={p}".format(p=name)
    page, html_code = tools.scrape(search_url)
    if not page.url.replace(main_site, '').startswith('players/'):
        potentials = []
        votes = {}
        if filters is not None:  # use other arguments to narrow search
            list_search = html_code.findAll("div", {"class": "search-item-name"})
            links = [main_site[:-1] + ls.find('a')['href'] for ls in list_search
                     if ls.find('a')['href'].startswith('/players/')]
            if 'year' in filters.keys():
                eligible_years = []
                for ls in list_search:
                    text_ls = ls.get_text()
                    if text_ls.find('(') != -1:
                        year_range = text_ls[text_ls.find('(')+1:text_ls.find(')')]
                        if '-' in year_range:
                            [begin, end] = year_range.split('-')
                        else:
                            begin, end = year_range, year_range
                        year_test = range(int(begin), int(end) + 1)
                    else:
                        year_test = []
                    if filters['year'] in year_test:
                        eligible_years.append(main_site[:-1] + ls.find('a')['href'])
                links = [link for link in links if link in eligible_years]
            if len(links) == 0:
                links = google_player_page(name)
            for potential_url in links:
                voting_dict = {}
                game_df, positions = get_game_log(potential_url)
                if 'position' in filters.keys():
                    if filters['position'].upper() in positions:
                        voting_dict['position'] = 1
                    else:
                        voting_dict['position'] = None
                if 'year' in filters.keys():
                    voting_dict['year'] = None
                    if 'year_id' in game_df.columns:
                        if filters['year'] in game_df['year_id'].values:
                            voting_dict['year'] = 1
                if 'team' in filters.keys():
                    voting_dict['team'] = None
                    if 'team' in game_df.columns:
                        target_id = exp.team_search(filters['team'])
                        all_teams = game_df['team'].unique()
                        team_ids = [exp.team_search(at) for at in all_teams]
                        if target_id in team_ids:
                            voting_dict['team'] = 1
                if ('team' in filters.keys()) and ('year' in filters.keys()):
                    voting_dict['year_team'] = None
                    if 'team' in game_df.columns:
                        target_id = exp.team_search(filters['team'])
                        filtered_df = game_df[game_df['year_id'] == filters['year']]
                        all_teams = filtered_df['team'].unique()
                        team_ids = [exp.team_search(at) for at in all_teams]
                        if target_id in team_ids:
                            voting_dict['year_team'] = 1
                if all(k in filters.keys() for k in ('year', 'platform', 'date')):
                    voting_dict['log_exists'] = None
                    points = get_fantasy_points(potential_url, filters['year'], filters['platform']).dropna()
                    if filters['date'] in points.index:
                        voting_dict['log_exists'] = 1
                        if ('date' in filters.keys()) and ('points' in filters.keys()):
                            voting_dict['log_match'] = None
                            if filters['points'] == points.ix[filters['date']]:
                                voting_dict['log_match'] = 1
                votes[potential_url] = voting_dict.copy()
                potentials.append(potential_url)
        votes = pd.DataFrame(votes).T
        orders = ['year', 'team', 'year_team', 'position', 'log_exists', 'log_match', 'google']
        found = False
        for order in orders:
            if len(potentials) <= 1:
                found = True
            if (not found) and (order in votes.columns):
                eligible = votes[order].dropna().index
                test_potentials = [p for p in potentials if p in eligible]
                if len(test_potentials) > 0:
                    potentials = test_potentials
            if (not found) and (order == 'google'):
                links_google = google_player_page(name)
                test_potentials = [p for p in potentials if p in links_google]
                if len(test_potentials) > 0:
                    potentials = test_potentials
        if (len(potentials) != 1) and allow_missing:
            print("Ignoring missing player id for : " + str((name, filters)) + str(potentials))
            return None, None
        assert len(potentials) == 1, "Need more precise filtering for: " + str((name, filters)) + str(potentials)
        page, html_code = tools.scrape(potentials[0])
    str_replace = ' Stats | Pro-Football-Reference.com'
    titles = html_code.find_all('title')
    if len(titles) > 0:
        player_name = titles[0].get_text().replace(str_replace, '').strip()
    else:
        print("Page is down for " + name + ", using override. Double-check here")
        player_name = name
    return player_name, page.url


def find_team_page(name):
    text = 'profootballreference team ' + name
    team_urls = gl.link_results(text)
    team_name = None
    found = False
    for tu in team_urls:
        splitter = tu.split('/')
        if tu.startswith(main_site + 'teams/') and (len(splitter) == 6) and not found:
            team_name = tu.replace(main_site + 'teams/', '').split('/')[0]
            found = True
    assert found, "Google results doesn't work for: " + text
    return team_name.lower(), main_site + 'teams/' + team_name + '/'


def google_player_page(name):
    player_urls = gl.link_results('profootballreference player ' + name)
    list_return = []
    for pu in player_urls:
        splitter = pu.split('/')
        if pu.startswith(main_site + 'players/') and (len(splitter) == 6) and pu.endswith('.htm'):
            list_return.append(pu)
    return list_return


def get_game_log(player_page):
    team = False
    if player_page.startswith('https://www.pro-football-reference.com/teams/'):
        team = True
        all_tables = []
        positions = ['DST']
        for year in load.years:
            page, html_code = tools.scrape(player_page + str(year) + "/gamelog/")
            all_tables = all_tables + [html_code.find_all('tbody')[0]]
    else:
        page, html_code = tools.scrape(player_page.replace('.htm', '/gamelog/'))
        all_tables = html_code.find_all('tbody')
        positions = []
        html_descriptions = html_code.find_all('div', {'class': 'players'})[0].find_all('p')
        for descrip in html_descriptions:
            if descrip.find('strong') is not None:
                if descrip.find('strong').get_text() == 'Position':
                    line_breaks = descrip.get_text().split('\n')
                    for lb in line_breaks:
                        if lb.startswith('Position: '):
                            positions = lb.replace('Position: ', '').split('-')
    append_df = pd.DataFrame()
    count = 0
    for at in all_tables:
        game_rows = at.find_all('tr')
        player_map_df = {}
        for game_row in game_rows:
            if team:
                game_data = {'year_id': game_row.get('id').split('.')[0].replace('gamelog', ''),
                             'team': player_page.replace(main_site + 'teams/', '').replace('/', '')}
            else:
                game_data = {}
            for ct in game_row.contents:
                if ct != '\n':
                    item = ct.get('data-stat')
                    value = ct.get_text()
                    if (item == 'game_date') and (value != 'Date'):
                        if team:
                            date_timer = pd.to_datetime(value + ', ' + game_data['year_id'])
                        else:
                            date_timer = pd.to_datetime(value)
                        game_data[item] = date_timer.strftime('%Y-%m-%d')
                    else:
                        game_data[item] = value if value != '' else None
                    if ct.find('a') is not None:
                        game_data[item + '_link'] = main_site[:-1] + ct.find('a')['href']
            if team:
                insert_row = (game_data['boxscore_word'] != 'preview')
            else:
                insert_row = (game_data['ranker'] != 'Rk')
            if insert_row:
                player_map_df[count] = game_data
                count = count + 1
        append_df = append_df.append(pd.DataFrame(player_map_df).T, ignore_index=True, sort=True)
    if len(append_df) > 0:
        type_dict = {'year_id': int, 'week_num': int}
        append_df = append_df.astype(type_dict)
    # descriptors
    return append_df, positions


def get_fantasy_points(player_page, year, platform):
    assert platform in ('fd', 'dk'), "Incorrect input into platform argument"
    table_stats = tools.html_pandas(player_page.replace('.htm', '/fantasy/{year}/'.format(year=year)))[0]
    ret_df = {'date': table_stats.iloc[:, table_stats.columns.get_level_values(2) == 'Date'].ix[:,
                      0],
              'points': table_stats.iloc[:, table_stats.columns.get_level_values(2) == (platform.upper() + 'Pt')].ix[:,
                        0]}
    return pd.DataFrame(ret_df).set_index('date')['points']
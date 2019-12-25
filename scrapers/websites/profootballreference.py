import scrapers.toolkit as tools
import scrapers.websites.google as gl
import data.explorer as exp
import pandas as pd
import data.loader as load

main_site = "https://www.pro-football-reference.com/"


def find_player_page(name, filters=None):
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
                votes[potential_url] = voting_dict.copy()
                potentials.append(potential_url)
        votes = pd.DataFrame(votes).T
        orders = ['year', 'team', 'year_team', 'position', 'google']
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
                links = google_player_page(name)
                test_potentials = [p for p in potentials if p in links]
                if len(test_potentials) > 0:
                    potentials = test_potentials
        assert len(potentials) == 1, "Need more precise filtering for: " + str((name, filters)) + str(potentials)
        page, html_code = tools.scrape(potentials[0])
    str_replace = ' Stats | Pro-Football-Reference.com'
    player_name = html_code.find_all('title')[0].get_text().replace(str_replace, '').strip()
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
        if pu.startswith(main_site + 'players/') and (len(splitter) == 6):
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

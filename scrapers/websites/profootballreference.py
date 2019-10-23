import scrapers.toolkit as tools
import scrapers.websites.google as gl

main_site = "https://www.pro-football-reference.com/"


def find_player_page(name, filters=None):
    search_url = main_site + 'search/search.fcgi?search={p}'.format(p=name)
    page, html_code = tools.scrape(search_url)
    if not page.url.replace(main_site, '').startswith('players/'):
        potentials = []
        if filters is not None:  # use other arguments to narrow search
            year = filters['year']
            position = filters['position']
            list_search = html_code.findAll("div", {"class": "search-item-name"})
            for ls in list_search:
                str_split = ls.get_text().split()
                loc = -1  # loc represents index for years played
                for ix in range(len(str_split)):
                    if str_split[ix].startswith('('):
                        loc = ix
                years_str = str_split[loc][1:-1].split('-')
                if len(years_str) > 1:
                    years = list(range(int(years_str[0]), int(years_str[1]) + 1))
                else:
                    years = [int(years_str[0])]
                if year not in years:
                    continue
                list_pos = str_split[loc - 1].split('-')
                if position not in list_pos:
                    continue
                potentials.append(ls.find('a')['href'])
        assert len(potentials) == 1, "Need more precise filtering for: " + str((name, filters))
        page, html_code = tools.search(main_site + potentials[0])
    str_replace = ' Stats | Pro-Football-Reference.com'
    player_name = html_code.find_all('title')[0].get_text().replace(str_replace, '')
    return player_name, page.url


def find_team_page(name):
    text = 'profootballreference team ' + name
    team_url = gl.first_link_result(text)
    assert team_url.startswith(main_site + 'teams/'), "Google result doesn't work: " + team_url
    team_name = team_url.replace(main_site + 'teams/', '').split('/')[0]
    return team_name, main_site + 'teams/' + team_name + '/'
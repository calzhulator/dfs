import pandas as pd
import data.databaseManager as dbMgr
import scrapers.websites.profootballreference as pfr


def player_search(name_search, auto_insert=True, dst_mode=False, filters=None,
                  source=None, sourceid=None):
    insert_source = False
    if source is not None and sourceid is not None:
        alias_qry = """select playerid from playerAliases where source like ? and sourceid like ?"""
        alias_df = dbMgr.query(alias_qry, (source, sourceid))
        if len(alias_df['playerid']) == 1:
            return alias_df['playerid'][0]
        insert_source = True
    if dst_mode:
        if filters is not None:
            if 'team' in filters.keys():
                name_search = filters['team']
        teamid = team_search(name_search, auto_insert=auto_insert)
        str_qry = "select name, link from teams where teamid like ?"
        qry_df = dbMgr.query(str_qry, (teamid,))
        player_name = qry_df['name'][0]
        link = qry_df['link'][0]
    else:
        player_name, link = pfr.find_player_page(name_search, filters)
    str_qry = "select playerid from players where link like ?"
    qry_df = dbMgr.query(str_qry, (link,))
    if len(qry_df['playerid']) == 0:
        to_insert = pd.Series({'name': player_name,
                               'link': link,
                               'playerid': None})
        dbMgr.series_insert(to_insert, 'players', auto_insert)
        qry_df = dbMgr.query(str_qry, (link,))
    if insert_source:
        to_insert = pd.Series({'source': source,
                               'sourceid': sourceid,
                               'playerid': qry_df['playerid'][0]})
        dbMgr.series_insert(to_insert, 'playerAliases', True)
    return qry_df['playerid'][0]


def team_search(name_search, auto_insert=True):
    name_search = name_search.lower()
    alias_qry = "select teamid from teamAliases where teamalias = ?"
    alias_df = dbMgr.query(alias_qry, (name_search,))
    if len(alias_df['teamid']) == 1:
        return alias_df['teamid'][0]
    team_name, link = pfr.find_team_page(name_search)
    str_qry = "select teamid from teams where link like ?"
    qry_df = dbMgr.query(str_qry, (link,))
    if len(qry_df['teamid']) == 0:
        team_insert = pd.Series({'name': team_name,
                                 'link': link,
                                 'teamid': None})
        dbMgr.series_insert(team_insert, 'teams', auto_insert)
        qry_df = dbMgr.query(str_qry, (link,))
    alias_insert = pd.Series({'teamalias': name_search,
                              'teamid': int(qry_df['teamid'][0])})
    dbMgr.series_insert(alias_insert, 'teamAliases', True)
    return qry_df['teamid'][0]


def time_search(season, week):
    str_qry = 'select timeid from time where season = ? and week = ?'
    qry_df = dbMgr.query(str_qry, (season, week))
    if len(qry_df['timeid']) == 0:
        to_insert = pd.Series({'season': season,
                               'week': week,
                               'timeid': None})
        dbMgr.series_insert(to_insert, 'time', True)
        qry_df = dbMgr.query(str_qry, (season, week))
    return qry_df['timeid'][0]


def contest_search(platform, description):
    str_qry = """select contestid from contestStructure
            where platform = ? and description = ?"""
    qry_df = dbMgr.query(str_qry, (platform, description))
    if len(qry_df['contestid']) == 0:
        to_insert = pd.Series({'platform': platform,
                               'description': description,
                               'contestid': None})
        dbMgr.series_insert(to_insert, 'contestStructure', True)
        qry_df = dbMgr.query(str_qry, (platform, description))
    return qry_df['contestid'][0]


def game_insert(link, timeid, hometeamid, awayteamid):
    str_qry = 'select gameid from games where link = ?'
    qry_df = dbMgr.query(str_qry, (link,))
    if len(qry_df['gameid']) == 0:
        if None in (timeid, hometeamid, awayteamid):
            assert None not in (timeid, hometeamid, awayteamid), "Incorrect inputs into game insert function"
        to_insert = pd.Series({'link': link,
                               'timeid': timeid,
                               'hometeamid': hometeamid,
                               'awayteamid': awayteamid})
        dbMgr.series_insert(to_insert, 'games', True)
        qry_df = dbMgr.query(str_qry, (link,))
    return qry_df['gameid'][0]


def game_search(timeid, playerid=None, teamid=None):
    assert (playerid is not None) or (teamid is not None), "Incorrect inputs into game search function"
    if playerid is not None:
        str_qry = """select gameid from games where timeid like ? and  like ?"""
        qry_df = dbMgr.query(str_qry, (link,))


# def get_player_gamelog(playerid):
#     str_qry = "select link from players where playerid = ?"
#     player_link = dbMgr.query(str_qry, (playerid,))['link'][0]
#     gamelog_df = pfr.get_game_log(player_link)
#     return gamelog_df
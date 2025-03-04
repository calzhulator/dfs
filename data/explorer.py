import pandas as pd
import data.databaseManager as dbMgr
import scrapers.websites.profootballreference as pfr


def player_search(name_search, auto_insert=True, dst_mode=False, filters=None,
                  source=None, sourceid=None, allow_missing=False):
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
        teamid = team_search(name_search)
        str_qry = "select name, link from teams where teamid like ?"
        qry_df = dbMgr.query(str_qry, (teamid,))
        player_name = qry_df['name'][0]
        link = qry_df['link'][0]
    else:
        player_name, link = pfr.find_player_page(name_search, filters, allow_missing)
        if link is None:
            return None
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
    if name_search == 'fa':  # fantasy pros
        return -999
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


def time_search(season=None, week=None, date=None):
    if None not in (season, week):
        str_qry = 'select timeid from time where season = ? and week = ?'
        qry_df = dbMgr.query(str_qry, (season, week))
        if len(qry_df['timeid']) == 0:
            to_insert = pd.Series({'season': season,
                                   'week': week,
                                   'timeid': None})
            dbMgr.series_insert(to_insert, 'time', True)
            qry_df = dbMgr.query(str_qry, (season, week))
    elif date is not None:
        str_qry = 'select distinct timeid from games where date = ?'
        qry_df = dbMgr.query(str_qry, (date,))
    assert len(qry_df['timeid']) == 1, "Times are not loaded properly"
    return qry_df['timeid'][0]


def structure_search(platform, scoring, description):
    str_qry = """select structureid from contestStructure where platform = ? and scoring = ? and description = ?"""
    qry_df = dbMgr.query(str_qry, (platform, scoring, description))
    if len(qry_df['structureid']) == 0:
        to_insert = pd.Series({'platform': platform,
                               'scoring': scoring,
                               'description': description,
                               'structureid': None})
        dbMgr.series_insert(to_insert, 'contestStructure', True)
        qry_df = dbMgr.query(str_qry, (platform, scoring, description))
    structure_constraints(qry_df['structureid'][0], platform, description)
    return qry_df['structureid'][0]


def structure_constraints(structureid, platform, description):
    str_qry = """select structureid from structureConstraints where structureid = ?"""
    qry_df = dbMgr.query(str_qry, (structureid,))
    constraint_df = None
    if len(qry_df['structureid']) == 0:
        if platform == 'fanduel':
            if description == 'all week without kicker':
                list_constraints = [("bound", "<=", None, 1), ("bound", ">=", None, 0), ("dot", "==", 'DST', 1),
                    ("dot", "==", 'QB', 1), ("dot", ">=", 'RB', 2), ("dot", "<=", 'RB', 3), ("dot", ">=", 'WR', 3),
                    ("dot", "<=", 'WR', 4), ("dot", ">=", 'TE', 1), ("dot", "<=", 'TE', 2), ("dot", "==", 'Flex', 7),
                    ("dot", "<=", 'salary', 60000)]
            elif description == 'all week with kicker':
                list_constraints = [("bound", "<=", None, 1), ("bound", ">=", None, 0), ("dot", "==", 'DST', 1),
                    ("dot", "==", 'QB', 1), ("dot", "==", 'RB', 2), ("dot", "==", 'WR', 3), ("dot", "==", 'TE', 1),
                    ("dot", "<=", 'salary', 60000), ("dot", "==", 'K', 1)]
        elif platform == 'draftkings':
            list_constraints = [("bound", "<=", None, 1), ("bound", ">=", None, 0), ("dot", "==", 'DST', 1),
                    ("dot", "==", 'QB', 1), ("dot", ">=", 'RB', 2), ("dot", "<=", 'RB', 3), ("dot", ">=", 'WR', 3),
                    ("dot", "<=", 'WR', 4), ("dot", ">=", 'TE', 1), ("dot", "<=", 'TE', 2), ("dot", "==", 'Flex', 7),
                    ("dot", "<=", 'salary', 50000)]
        elif platform == 'yahoo':
            list_constraints = [("bound", "<=", None, 1), ("bound", ">=", None, 0), ("dot", "==", 'DST', 1),
                                ("dot", "==", 'QB', 1), ("dot", ">=", 'RB', 2), ("dot", "<=", 'RB', 3),
                                ("dot", ">=", 'WR', 3), ("dot", "<=", 'WR', 4), ("dot", ">=", 'TE', 1),
                                ("dot", "<=", 'TE', 2), ("dot", "==", 'Flex', 7), ("dot", "<=", 'salary', 200)]
        constraint_df = pd.DataFrame(list_constraints)
    if constraint_df is not None:
        constraint_df.columns = ['type', 'operator', 'vec', 'bound']
        constraint_df['structureid'] = structureid
        dbMgr.df_insert(constraint_df, 'structureConstraints', True)
    return None


def game_insert(link, timeid=None, hometeamid=None, awayteamid=None, date=None):
    str_qry = 'select gameid from games where link = ?'
    qry_df = dbMgr.query(str_qry, (link,))
    if len(qry_df['gameid']) == 0:
        assert None not in (timeid, hometeamid, awayteamid, date), "Incorrect inputs into game insert function"
        to_insert = pd.Series({'link': link,
                               'timeid': timeid,
                               'hometeamid': hometeamid,
                               'awayteamid': awayteamid,
                               'date': date})
        dbMgr.series_insert(to_insert, 'games', True)
        qry_df = dbMgr.query(str_qry, (link,))
    return qry_df['gameid'][0]


def contest_insert(link, timeid=None, structureid=None):
    str_qry = 'select contestid from contests where link = ?'
    qry_df = dbMgr.query(str_qry, (link,))
    if len(qry_df['contestid']) == 0:
        assert None not in (timeid, structureid), "Incorrect inputs into contest insert function"
        to_insert = pd.Series({'link': link,
                               'timeid': timeid,
                               'structureid': structureid})
        dbMgr.series_insert(to_insert, 'contests', True)
        qry_df = dbMgr.query(str_qry, (link,))
    return qry_df['contestid'][0]

# def game_search(timeid, playerid=None, teamid=None):
#     assert (playerid is not None) or (teamid is not None), "Incorrect inputs into game search function"
#     if playerid is not None:
#         str_qry = """select gameid from games where timeid like ? and link like ?"""
#         qry_df = dbMgr.query(str_qry, (link,))


# def get_player_gamelog(playerid):
#     str_qry = "select link from players where playerid = ?"
#     player_link = dbMgr.query(str_qry, (playerid,))['link'][0]
#     gamelog_df = pfr.get_game_log(player_link)
#     return gamelog_df

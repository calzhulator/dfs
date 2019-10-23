import pandas as pd
import data.databaseManager as dbMgr
import scrapers.websites.profootballreference as pfr


def player_search(name_search, auto_insert=False, filters=None):
    player_name, link = pfr.find_player_page(name_search, filters)
    str_qry = "select playerid from players where link like '{link}'".format(link=link)
    qry_df = dbMgr.query(str_qry)
    if len(qry_df['playerid']) == 0:
        to_insert = pd.Series({'name': player_name,
                               'link': link,
                               'playerid': None})
        dbMgr.series_insert(to_insert, 'players', auto_insert)
        qry_df = dbMgr.query(str_qry)
    return qry_df['playerid'][0]


def team_search(name_search, auto_insert=False):
    alias_qry = "select teamid from teams where teamalias = '{ns}'".format(ns=name_search)
    alias_df = dbMgr.query(alias_qry)
    if len(alias_df['teamid']) > 0:
        return alias_df['teamid'][0]
    team_name, link = pfr.find_team_page(name_search)
    str_qry = "select teamid from teams where link like '{link}'".format(link=link)
    qry_df = dbMgr.query(str_qry)
    if len(qry_df['teamid']) == 0:
        team_insert = pd.Series({'name': team_name,
                                 'link': link,
                                 'teamid': None})
        dbMgr.series_insert(team_insert, 'teams', auto_insert)
        qry_df = dbMgr.query(str_qry)
    alias_insert = pd.Series({'teamalias': name_search,
                              'teamid': qry_df['teamid'][0]})
    dbMgr.series_insert(alias_insert, 'teamAliases', auto_insert)
    return qry_df['teamid'][0]

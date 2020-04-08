import sqlite3
import pandas as pd
import numpy as np

sqlite3.register_adapter(np.int64, lambda val: int(val))
conn = sqlite3.connect('dataPROD.db')
c = conn.cursor()


def __inception():
    # team and player units
    c.execute('''CREATE TABLE IF NOT EXISTS players
                (playerid integer primary key,
                link varchar,
                name varchar)''')
    c.execute('''CREATE TABLE IF NOT EXISTS playerAliases
                 (source varchar,
                 sourceid varchar,
                 playerid integer,
                 primary key (source, sourceid))''')
    c.execute('''CREATE TABLE IF NOT EXISTS teams
                 (teamid integer primary key,
                 link varchar,
                 name varchar)''')
    c.execute('''CREATE TABLE IF NOT EXISTS teamAliases
                 (teamalias varchar primary key,
                 teamid integer)''')
    c.execute('''CREATE TABLE IF NOT EXISTS time
                 (timeid integer primary key,
                 season integer,
                 week integer)''')
    c.execute('''CREATE TABLE IF NOT EXISTS contests
                 (contestid integer primary key,
                 timeid integer,
                 link varchar,
                 structureid integer)''')
    c.execute('''CREATE TABLE IF NOT EXISTS contestStats
                 (contestid integer,
                 stat varchar,
                 value float,
                 primary key (contestid, stat))''')
    c.execute('''CREATE TABLE IF NOT EXISTS contestStructure
                 (structureid integer primary key,
                 platform varchar,
                 scoring varchar,
                 description varchar)''')
    c.execute('''CREATE TABLE IF NOT EXISTS contestPlayers
                 (structureid integer,
                 playerid integer,
                 timeid integer,
                 position varchar,
                 salary float,
                 points float,
                 primary key (structureid, playerid, timeid, position))''')
    c.execute('''CREATE TABLE IF NOT EXISTS games
                 (gameid integer primary key,
                 link varchar,
                 timeid integer,
                 hometeamid integer,
                 awayteamid integer,
                 date text)''')
    c.execute('''CREATE TABLE IF NOT EXISTS gameLog
                 (gameid integer,
                 playerid integer,
                 stat varchar,
                 value float,
                 primary key (gameid, playerid, stat))''')
    c.execute('''CREATE TABLE IF NOT EXISTS playsForTeam
                (gameid integer,
                playerid integer,
                teamid integer,
                oppid integer,
                primary key (gameid, playerid))''')
    c.execute('''CREATE TABLE IF NOT EXISTS projections
                (timeid integer,
                playerid integer,
                source varchar,
                statistic varchar,
                value float,
                primary key (timeid, playerid, source, statistic))''')
    c.execute('''CREATE TABLE IF NOT EXISTS contestOwnership
                (contestid integer,
                playerid integer,
                value float,
                primary key (contestid, playerid))''')
    c.execute('''CREATE TABLE IF NOT EXISTS structureConstraints
                 (structureid integer,
                 type varchar,
                 operator varchar,
                 vec varchar,
                 bound float,
                 primary key (structureid, type, operator, vec))''')
    return None


__inception()


def series_insert(s_insert, table, auto_insert=False):
    to_insert = pd.DataFrame(s_insert).T
    return df_insert(to_insert, table, auto_insert)


def df_insert(d_insert, table, auto_insert=False):
    if not auto_insert:
        pd.set_option('display.max_colwidth', -1)
        print(d_insert)
        user_in = input("Insert into table {t}? Type yes: ".format(t=table))
        assert user_in == 'yes', "Not inserting, break"
    d_insert.to_sql(table, conn, if_exists='append', index=False)
    return d_insert


def query(str_qry, params=None):
    return pd.read_sql(str_qry, conn, params=params)


def truncate(list_trunc=None):
    list_tables = query("""select name FROM sqlite_master WHERE type = 'table' 
                            AND name NOT IN ('teams', 'teamAliases', 'players', 'playerAliases')""")
    for table in list_tables['name'].values:
        if list_trunc is not None:
            if table in list_trunc:
                c.execute("drop table if exists {t};".format(t=table))
        else:
            c.execute("drop table if exists {t};".format(t=table))
    conn.commit()
    return None


def list_overrides():
    print("""Steve Smiths before loading fantasypros projections

    insert into playerAliases (source, sourceid, playerid)
    select 'fantasypros', 9579, playerid from players
    where link like 'https://www.pro-football-reference.com/players/S/SmitSt01.htm'
    
    insert into playerAliases (source, sourceid, playerid)
    select 'fantasypros', 9580, playerid from players
    where link like 'https://www.pro-football-reference.com/players/S/SmitSt02.htm'
    """)
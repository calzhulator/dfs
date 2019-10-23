import sqlite3
import pandas as pd
conn = sqlite3.connect('dataPROD.db')
c = conn.cursor()


def __inception():
    # team and player units
    c.execute('''CREATE TABLE teams
                 (teamid integer primary key,
                 link varchar,
                 name varchar)''')
    c.execute('''CREATE TABLE players
                 (playerid integer primary key,
                 link varchar,
                 name varchar)''')
    c.execute('''CREATE TABLE teamAliases
                 (teamalias varchar primary key,
                 teamid integer)''')
    c.execute('''CREATE TABLE pfrTimeMapper
                 (season integer,
                 teamid integer,
                 gamenumber integer,
                 time integer,
                 primary key (season, teamid, gamenumber))''')
    c.execute('''CREATE TABLE mapPlayerTeam
                 (season integer,
                 time integer,
                 playerid integer,
                 teamid integer,
                 primary key (season, time, playerid))''')


def series_insert(s_insert, table, auto_insert=False):
    to_insert = pd.DataFrame(s_insert).T
    return df_insert(to_insert, table, auto_insert)


def df_insert(d_insert, table, auto_insert=False):
    if not auto_insert:
        print(d_insert)
        user_in = input("Insert? Type yes: ")
        assert user_in == 'yes', "Not inserting, break"
    d_insert.to_sql(table, conn, if_exists='append', index=False)
    return d_insert


def query(str_qry):
    return pd.read_sql(str_qry, conn)
import scrapers.websites.rotoguru as rg
import data.explorer as exp
import pandas as pd
import data.databaseManager as dbMgr
import scrapers.websites.profootballreference as pfr
import scrapers.websites.fantasypros as fp
import scrapers.websites.fftoday as fft
import numpy as np

pd.options.mode.chained_assignment = None

years = range(2011, 2020)
platforms = rg.site_mappings.keys()
weeks = range(1, 18)
projections_sources = {'fantasypros': fp, 'fftoday': fft}


def load_salaries(historical=False):
    if historical:
        all_data = pd.DataFrame()
        for year in years:
            for week in weeks:
                for plat in platforms:
                    platform_df = rg.get_platform_data(plat, year, week)
                    if platform_df is not None:
                        platform_df['time_id'] = exp.time_search(year, week)
                        if plat == 'draftkings':
                            scoring_mode = 'FULL'
                        elif plat in ('yahoo', 'fanduel'):
                            scoring_mode = 'HALF'
                        if (year < 2018) and (plat == 'fanduel'):
                            platform_df['structureid'] = exp.structure_search(plat, scoring_mode,
                                                                              'all week with kicker')
                        else:
                            platform_df['structureid'] = exp.structure_search(plat, scoring_mode,
                                                                              'all week without kicker')
                        all_data = all_data.append(platform_df, ignore_index=True, sort=False)
    else:
        1 + 1
    loop_df = {}
    for ix, row in all_data.iterrows():
        row_df = {}
        dst_mode = row['Pos'] == 'DST'
        filters = {'year': row['Year'], 'team': row['Team'], 'position': row['Pos']}
        row_df['playerid'] = exp.player_search(row['Name'], dst_mode=dst_mode, filters=filters, auto_insert=True)
        row_df['structureid'] = row['structureid']
        row_df['timeid'] = row['time_id']
        row_df['position'] = row['Pos']
        row_df['salary'] = row['salary']
        row_df['points'] = row['points']
        loop_df[ix] = row_df
    loop_df = pd.DataFrame(loop_df).T
    dbMgr.df_insert(loop_df, 'contestPlayers', True)
    return None


def load_game_logs(historical=False):
    if historical:
        players_df = dbMgr.query("""select playerid, link, name from players
                                    where playerid not in (
                                    select distinct playerid from playsForTeam)""")
        for ix, row in players_df.iterrows():
            team = False
            if row['link'].startswith('https://www.pro-football-reference.com/teams/'):
                team = True
            gamelog_df, position = pfr.get_game_log(row['link'])
            if len(gamelog_df) > 0:
                gamelog_df['playerid'] = row['playerid']
                gamelog_df['timeid'] = gamelog_df.apply(lambda x: exp.time_search(x.year_id, x.week_num), axis=1)
                gamelog_df['teamid'] = gamelog_df['team'].apply(lambda x: exp.team_search(x))
                gamelog_df['oppid'] = gamelog_df['opp'].apply(lambda x: exp.team_search(x))
                gamelog_df['hometeamid'] = gamelog_df.apply(lambda x: exp.team_search(x.opp) if x.game_location == '@'
                else exp.team_search(x.team), axis=1)
                gamelog_df['awayteamid'] = gamelog_df.apply(lambda x: exp.team_search(x.team) if x.game_location == '@'
                else exp.team_search(x.opp), axis=1)
                if team:
                    gamelog_df['gameid'] = gamelog_df.apply(lambda x: exp.game_insert(x.boxscore_word_link, x.timeid,
                                                                                      x.hometeamid, x.awayteamid,
                                                                                      x.game_date), axis=1)
                else:
                    gamelog_df['gameid'] = gamelog_df.apply(lambda x: exp.game_insert(x.game_result_link, x.timeid,
                                                                                      x.hometeamid, x.awayteamid,
                                                                                      x.game_date), axis=1)
                dbMgr.df_insert(gamelog_df[['gameid', 'playerid', 'teamid', 'oppid']], 'playsForTeam', True)
                if not team:
                    stats = ['age', 'fumbles', 'fumbles_forced', 'fumbles_lost', 'fumbles_rec_td', 'fumbles_rec_yds',
                             'pass_att', 'pass_cmp', 'pass_int', 'pass_rating', 'pass_sacked', 'pass_sacked_yds',
                             'pass_td',
                             'pass_yds', 'rec', 'rec_td', 'rec_yds', 'rush_att', 'rush_td', 'rush_yds', 'targets']
                    indexer = ['gameid', 'playerid']
                    stacked_df = gamelog_df.reindex(columns=(stats + indexer)).astype(float).groupby(
                        ['gameid', 'playerid']).mean().stack().dropna().reset_index()
                    stacked_df.columns = ['gameid', 'playerid', 'stat', 'value']
                    dbMgr.df_insert(stacked_df, 'gameLog', True)
    else:
        1 + 1
    return None


def load_projections(historical=False):
    for proj_source, proj_class in projections_sources.items():
        if historical:
            all_data = pd.DataFrame()
            for year in years:
                for week in weeks:
                    exist_df = dbMgr.query("""select playerid from projections proj
                                            join time t on proj.timeid = t.timeid
                                            where season = {}
                                            and week = {}
                                            and source = '{}'""".format(year, week, proj_source))
                    if len(exist_df) == 0:
                        platform_df = proj_class.get_projections(year, week)
                        if platform_df is not None:
                            platform_df['timeid'] = exp.time_search(year, week)
                            platform_df['source'] = proj_source
                            all_data = all_data.append(platform_df, ignore_index=True, sort=False)
        else:
            1 + 1
        if len(all_data) > 0:
            all_data['playerid'] = all_data.apply(lambda x: exp.player_search(x.PLAYER, dst_mode=(x.position == 'DST'),
                                                                              filters={'position': x.position,
                                                                                       'year': x.year,
                                                                                       'team': x.TEAM},
                                                                              auto_insert=True, source=proj_source,
                                                                              sourceid=x.SOURCE, allow_missing=True),
                                                  axis=1)
            all_data = all_data[all_data['playerid'].notnull()]
            stats = list(set(sum(proj_class.positions.values(), [])).union({'FPTS_HALF', 'FPTS_STD', 'FPTS_FULL'}))
            stacked_df = all_data.groupby(['timeid', 'playerid', 'source'])[stats].mean().stack().dropna().reset_index()
            stacked_df.columns = ['timeid', 'playerid', 'source', 'statistic', 'value']
            dbMgr.df_insert(stacked_df, 'projections', True)
    return None


def load_ownership(historical=False):
    if historical:
        dates = dbMgr.query("""select distinct date, season from games g 
                        join time t on g.timeid = t.timeid order by date""")
        all_dates = dates[dates['date'] > '2016-09-10'].set_index('date', verify_integrity=True)['season']
        import pickle
        with open('ownership.pickle', 'rb') as handle:
            test_data = pickle.load(handle)
        day_df = {}
        for key in test_data.keys():
            if (test_data[key][0] is not None) and (pd.to_datetime(key).day_name() == 'Sunday'):
                possible_keys = [x for x in test_data[key][0].keys() if
                                 ('Classic' in x or 'SalaryCap' in x) and '1:00 pm' in x]
                if len(possible_keys) > 0:
                    temp_max = 0
                    temp_contest = None
                    for pk in possible_keys:
                        if test_data[key][0][pk].shape[0] > temp_max:
                            temp_max = test_data[key][0][pk].shape[0]
                            temp_contest = pk
                    if temp_contest is not None:
                        if len(test_data[key][1][temp_contest]) > 100:
                            day_df[key] = temp_contest
        for dd, val in day_df.items():
            contest_df = test_data[dd][0][val]
            ownership_df = test_data[dd][1][val]
            ownership_df.columns = [x.replace('$', '').replace(',', '') for x in ownership_df.columns]
            exclude_contests = contest_df['Name'].value_counts()[contest_df['Name'].value_counts() > 1].index
            filtered_df = ownership_df[ownership_df['Avg'] > 0.0001].drop(exclude_contests, axis=1, errors='ignore')
            valid_contests = [x for x in filtered_df.columns if x not in ['Player', 'Pos', 'Avg', 'Fpts']]
            valid_df = contest_df[
                contest_df['Name'].isin(valid_contests) & -contest_df['Name'].isin(exclude_contests)].drop_duplicates()
            timeid = exp.time_search(date=dd)
            structureid = exp.structure_search('draftkings', 'FULL', 'all week without kicker')
            valid_df['contestid'] = valid_df.apply(lambda x: exp.contest_insert(x.Link, timeid, structureid), axis=1)
            link_map = valid_df.set_index('Name')['contestid']
            pivoted_df = valid_df.set_index('contestid').drop(['Name', 'Link', 'Winner'],
                                                              axis=1).unstack().reset_index()
            pivoted_df.columns = ['stat', 'contestid', 'value']
            dbMgr.df_insert(pivoted_df[['contestid', 'stat', 'value']], 'contestStats', True)
            filtered_df = filtered_df[['Player', 'Pos', 'Avg', 'Fpts']+list(link_map.keys())]
            filtered_df.columns = [link_map[x]
                                   if x in valid_contests
                                   else x for x in filtered_df.columns]
            filtered_df['playerid'] = filtered_df.apply(
                lambda x: exp.player_search(x.Player, dst_mode=(x.Pos == 'DST'),
                                            filters={'position': x.Pos,
                                                     'year': all_dates[dd],
                                                     'points': x.Fpts,
                                                     'platform': 'dk',
                                                     'date': dd},
                                            auto_insert=True, allow_missing=True), axis=1)
            player_df = filtered_df.set_index('playerid').drop(['Player', 'Pos', 'Avg', 'Fpts'], axis=1)
            stacked_df = player_df.replace(0.0, np.nan).unstack().dropna().reset_index().drop_duplicates()
            stacked_df.columns = ['contestid', 'playerid', 'value']
            dbMgr.df_insert(stacked_df[['contestid', 'playerid', 'value']], 'contestOwnership', True)
    else:
        1 + 1
    return None

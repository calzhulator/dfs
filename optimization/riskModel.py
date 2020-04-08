import data.databaseManager as dbMgr
import optimization.projections as proj
import pandas as pd
from functools import lru_cache


@lru_cache(maxsize=None)
def get_position_cov(structureid):
    contest_data = dbMgr.query("""
                        select cp.playerid, t.timeid, season, week, position, points as actualPoints,
                        teamid, oppid, value as projectedPoints from contestPlayers cp
                        join contestStructure s on cp.structureid = s.structureid
                        join games g on g.timeid = cp.timeid
                        join playsForTeam pfr on pfr.gameid = g.gameid and pfr.playerid = cp.playerid
                        join projections proj on proj.timeid = cp.timeid and proj.playerid = cp.playerid
                        join time t on t.timeid = proj.timeid
                        where s.structureid = {}
                        and source = 'fantasypros'
                        and statistic = 'FPTS_' || s.scoring""".format(structureid))
    universe_data = dbMgr.query("""
                        select timeid, playerid, avg(value) as meanOwnership from contestOwnership co
                        join contests c on c.contestid = co.contestid
                        group by structureid, timeid, playerid""")
    filtered_data = universe_data.merge(contest_data, on=['timeid', 'playerid'])
    filtered_data['indRank'] = filtered_data.groupby(['timeid',
                                                      'teamid', 'position']).rank(ascending=False,
                                                                                  method='dense')['projectedPoints']
    filtered_data['posCategory'] = filtered_data['position'] + filtered_data['indRank'].astype(int).astype(str)
    count_positions = filtered_data.groupby('posCategory').count()['actualPoints']
    eligible_positions = count_positions[count_positions / filtered_data['actualPoints'].count() > .05].index
    eligible_data = filtered_data[filtered_data['posCategory'].isin(eligible_positions)]
    merged_data = eligible_data.merge(eligible_data, left_on=['timeid', 'teamid'], right_on=['timeid', 'teamid'])
    merged_data['side'] = 'Own'
    merged_data_opp = eligible_data.merge(eligible_data, left_on=['timeid', 'teamid'], right_on=['timeid', 'oppid'])
    merged_data_opp['side'] = 'Opposing'
    concat_data = merged_data.append(merged_data_opp, ignore_index=True, sort=False)
    return concat_data.groupby(['side',
                                'posCategory_x', 'posCategory_y'])[['actualPoints_x', 'actualPoints_y']].cov(
                                        ).iloc[0::2, -1].unstack(level=[3, 1])['actualPoints_x']


def get_player_cov(structureid, timeid):
    position_cov = get_position_cov(structureid)
    projections = proj.get_expected_points(structureid, timeid)
    player_mappings = dbMgr.query("""select cp.playerid, position,
                        teamid, oppid from contestPlayers cp
                        join contestStructure s on cp.structureid = s.structureid
                        join games g on g.timeid = cp.timeid
                        join playsForTeam pfr on pfr.gameid = g.gameid and pfr.playerid = cp.playerid
                        where s.structureid = {s} and cp.timeid = {t}""".format(
                                                s=structureid, t=timeid)).set_index('playerid')
    pm = player_mappings.join(projections, how='inner')
    pm['indRank'] = pm.groupby(['teamid', 'position']).rank(ascending=False, method='dense')['proj']
    pm['adjPosition'] = pm['position'] + pm['indRank'].astype(int).astype(str)
    filtered_data = pm[pm['adjPosition'].isin(list(position_cov.columns))]
    asset_max = {}
    for r1, row1 in filtered_data.iterrows():
        for r2, row2 in filtered_data.iterrows():
            if row1['teamid'] == row2['teamid']:
                asset_max[(r1, r2)] = position_cov.loc['Own'][row1['adjPosition']][row2['adjPosition']]
            elif row1['teamid'] == row2['oppid']:
                asset_max[(r1, r2)] = position_cov.loc['Opposing'][row1['adjPosition']][row2['adjPosition']]
            else:
                asset_max[(r1, r2)] = 0
    return pd.Series(asset_max).unstack(level=1)
import data.databaseManager as dbMgr


def get_position_cov():
    contest_data = dbMgr.query("""
                        select cp.playerid, s.structureid, t.timeid, season, week, position, points as actualPoints,
                        teamid, oppid, value as projectedPoints from contestPlayers cp
                        join contestStructure s on cp.structureid = s.structureid
                        join games g on g.timeid = cp.timeid
                        join playsForTeam pfr on pfr.gameid = g.gameid and pfr.playerid = cp.playerid
                        left join projections proj on proj.timeid = cp.timeid and proj.playerid = cp.playerid
                        join time t on t.timeid = proj.timeid
                        where s.platform = 'draftkings'
                        and (source = 'fftoday' or position like 'DST')
                        and statistic like 'FPTS_HALF'""")
    universe_data = dbMgr.query("""
                        select structureid, timeid, playerid, avg(value) as meanOwnership from contestOwnership co
                        join contests c on c.contestid = co.contestid
                        group by structureid, timeid, playerid""")
    filtered_data = universe_data.merge(contest_data, on=['structureid', 'timeid', 'playerid'])
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
                                'posCategory_x', 'posCategory_y'])[['actualPoints_x',
                                                                    'actualPoints_y']].corr().iloc[0::2, -1].unstack(
                                                                                                    level=[3, 1])
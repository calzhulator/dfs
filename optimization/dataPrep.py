import data.databaseManager as dbMgr
import pandas as pd
import optimization.riskModel as rm
import optimization.projections as proj
import optimization.dataPrep as prep


def get_opt_data(structureid, timeid):
    player_cov = rm.get_player_cov(structureid, timeid)
    ers = proj.get_expected_points(structureid, timeid)
    data_df = prep.get_platform_data(structureid, timeid)
    universe = player_cov.index.intersection(ers.index).intersection(data_df.index)
    er_uni = ers.reindex(universe)
    cov_uni = player_cov.reindex(universe, axis=1).reindex(universe, axis=0)
    data_uni = data_df.reindex(universe)
    list_constraints = prep.get_platform_constraints(structureid, data_uni)
    return er_uni, cov_uni, list_constraints


def get_platform_data(structureid, timeid):
    p_data = dbMgr.query("""select playerid, position, salary, points from contestPlayers cp
                        where timeid = {}
                        and structureid = {}""".format(timeid, structureid)).set_index('playerid')
    p_data = p_data.join(pd.get_dummies(p_data['position']))
    p_data['Flex'] = p_data['RB'] + p_data['TE'] + p_data['WR']
    return p_data


def get_platform_constraints(structureid, data_uni=None):
    constraint_df = dbMgr.query("""select type, operator, vec, bound from structureConstraints con
                            where structureid = {}""".format(structureid))
    if data_uni is not None:
        constraint_df['vec'] = constraint_df['vec'].apply(lambda x: data_uni[x] if x is not None else x)
    return list(constraint_df.itertuples(index=False, name=None))
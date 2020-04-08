import data.databaseManager as dbMgr


def get_expected_points(structureid, timeid):
    return get_projection(structureid, timeid, 'fantasypros')


def get_projection(structureid, timeid, source):
    scoring = dbMgr.query("""select distinct scoring from contestStructure where structureid = '{}'""".format(
                                                                        structureid))
    return dbMgr.query("""select playerid, value as proj from projections p
                        where timeid = {}
                        and statistic like 'FPTS_{}'
                        and source like '{}'""".format(
                    timeid, scoring['scoring'][0], source)).set_index('playerid')['proj']
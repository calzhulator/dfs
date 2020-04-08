import pandas as pd
import pulp as pulp
import optimization.dataPrep as prep
from cvxpy import *


def two_stage_opt(structureid, timeid, risk_param, num_contests):
    er_uni, cov_uni, list_constraints = prep.get_opt_data(structureid, timeid)
    opt_var = mean_variance(er_uni, cov_uni, risk_param, list_constraints)
    return linear_opt(er_uni, list_constraints, False, opt_var*num_contests, num_contests)


def non_integer_opt(structureid, timeid, risk_param):
    er_uni, cov_uni, list_constraints = prep.get_opt_data(structureid, timeid)
    return mean_variance(er_uni, cov_uni, risk_param, list_constraints)


def mean_variance(ers, cov, risk_param, list_constraints):
    assert len(ers.index) == len(cov.index), "mismatch of indices"
    assert len(cov.index) == len(cov.columns), "mismatch of indices"
    z_vec = Variable(len(ers.index), nonneg=True)
    z_constraints = []
    for lc in list_constraints:
        if lc[0] == "bound":
            if lc[1] == ">=":
                z_constraints.append(z_vec >= lc[3])
            if lc[1] == "<=":
                z_constraints.append(z_vec <= lc[3])
            if lc[1] == "==":
                z_constraints.append(z_vec == lc[3])
        elif lc[0] == "dot":
            if lc[1] == ">=":
                z_constraints.append(z_vec * lc[2] >= lc[3])
            if lc[1] == "<=":
                z_constraints.append(z_vec * lc[2] <= lc[3])
            if lc[1] == "==":
                z_constraints.append(z_vec * lc[2] == lc[3])
    prob = Problem(Maximize(sum(z_vec * ers) - risk_param * quad_form(z_vec, cov)),
                   z_constraints)
    prob.solve()
    return pd.Series(z_vec.value, index=ers.index)


def linear_opt(ers, list_constraints, max_mode=True, target_vec=None, num_contests=0):
    assert max_mode or target_vec is not None, "incorrect inputs"
    if max_mode or (num_contests == 0):
        num_contests = 1
        max_mode = True
    else:
        num_contests = num_contests
        max_mode = False
    choice_dict = {}
    for cont in range(num_contests):
        choice_dict[cont] = pulp.LpVariable.matrix("Choices" + str(cont), ers.index, lowBound=0, upBound=1,
                                                   cat='Integer')
    choice_dict['Total'] = pulp.LpVariable.matrix("ChoicesTotal", ers.index, lowBound=0, upBound=num_contests,
                                                  cat='Integer')
    choice_df = pd.DataFrame(choice_dict)
    if max_mode:
        prob = pulp.LpProblem("prob", pulp.LpMaximize)
        prob += pulp.lpDot(choice_dict['Total'], list(ers.values))
    else:
        prob = pulp.LpProblem("prob", pulp.LpMinimize)
        abs_y = pulp.LpVariable.matrix("abs_y", ers.index, lowBound=0)
        prob += pulp.lpSum(abs_y)  #  - pulp.lpDot(choice_dict['Total'], list(ers.values))
    for cont in range(num_contests):
        for lc in list_constraints:
            if lc[0] == "dot":
                if lc[1] == ">=":
                    prob += pulp.lpDot(list(lc[2].values), choice_dict[cont]) >= lc[3]
                if lc[1] == "<=":
                    prob += pulp.lpDot(list(lc[2].values), choice_dict[cont]) <= lc[3]
                if lc[1] == "==":
                    prob += pulp.lpDot(list(lc[2].values), choice_dict[cont]) == lc[3]
    for ix in choice_df.index:
        prob += pulp.lpSum(list(choice_df.iloc[ix].values[:num_contests])) == choice_df.iloc[ix]['Total']
        if not max_mode:
            prob += choice_df.iloc[ix]['Total'] - target_vec.values[ix] <= abs_y[ix]
            prob += choice_df.iloc[ix]['Total'] - target_vec.values[ix] >= -abs_y[ix]
    prob.solve()
    choice_df.index = ers.index
    solution_opt = choice_df.applymap(lambda x: bool(x.value())).drop('Total', axis=1).astype(int)
    if max_mode:
        return solution_opt[0]
    else:
        return solution_opt
from entities import Rule, Criterion
from copy import deepcopy
from datamart import Variable
import numpy as np
from tqdm import tqdm
import config 

def build_ranges(name, crn_ids):
    variable = Variable(name)
    variable.load()
    lowerbound, upperbound, step = variable.lowerbound, variable.upperbound, variable.step
    equal_value = None

    for crn_id in crn_ids:
        crn = Criterion(crn_id)
        crn.load()
        operator, value = crn.operator, crn.value
        if operator == '>':
            lowerbound = max(lowerbound, value + step) 
        elif operator == '>=':
            lowerbound = max(lowerbound, value) 
        elif operator == '<':
            upperbound = min(upperbound, value - step)
        elif operator == '<=':
            upperbound = min(upperbound, value)
        elif operator == "=":
            if equal_value is None:
                equal_value = value
            else:
                return ()
            
    if equal_value is not None:
        if lowerbound <= equal_value <= upperbound:
            return (equal_value, equal_value)
        else:
            return ()
        
    if lowerbound > upperbound:
        return ()

    return (lowerbound, upperbound)

            
def is_ranges_overlap(range1, range2):
    if isinstance(range1, tuple) and isinstance(range2, tuple) and len(range1) == 2 and len(range2) == 2:
        if (range2[0] <= range1[1]) and (range1[0] <= range2[1]):
            return max(range1[0], range2[0]), min(range1[1], range2[1])
    return None


def is_overlap(name, group_1, group_2):
    variable = Variable(name)
    variable.load()
    if variable.type == "categorical":
        all_options = set(variable.options)

        options1 = set()
        for crn_id in group_1:
            crn = Criterion(crn_id)
            crn.load()
            value = set(crn.value)
            if crn.operator == "in":
                options1 = options1.union(value)
            elif crn.operator == "not in":
                options1 = options1.union(all_options.difference(value))
        
        options2 = set()
        for crn_id in group_2:
            crn = Criterion(crn_id)
            crn.load()
            value = set(crn.value)
            if crn.operator == "in":
                options2 = options2.union(value)
            elif crn.operator == "not in":
                options2 = options2.union(all_options.difference(value))
        intersection = tuple(options1.intersection(options2))
        if len(intersection) == 0:
            return None
        return (name, intersection)
    else:
        range1 = build_ranges(name, group_1)
        range2 = build_ranges(name, group_2)
        overlap = is_ranges_overlap(range1, range2)
        if overlap is None:
            return None
        return (name, overlap)
     

def check_conflict_pair(rule1: Rule, rule2: Rule):
    results = []
    # Step 1: Build range from variable groups
    groups_1, groups_2 = {}, {}
    for c_id1 in rule1.criterion_ids:
        criterion1 = Criterion(c_id1)
        criterion1.load()
        name = criterion1.variable_name
        if name not in groups_1:
            groups_1[name] = []
        groups_1[name].append(c_id1)

    for c_id2 in rule2.criterion_ids:
        criterion2 = Criterion(c_id2)
        criterion2.load()
        name = criterion2.variable_name
        if name not in groups_2:
            groups_2[name] = []
        groups_2[name].append(c_id2)

    for name in groups_1:
        if name in groups_2:
            result = is_overlap(name, groups_1[name], groups_2[name])
            if result is None:
                return None
            else:
                results.append(result)
    return results


def random_by_idx(lb, ub, values, n_samples=100):
    values = [lb] + values + [ub]
    arr = np.random.choice(values, size=n_samples) + np.random.choice([0, 0.1], size=n_samples)
    return arr


def hit_in_rule(vtoi, sample, rule_id, bound):
    rule = Rule(rule_id)
    rule.load()
    # mọi feature chung giữa crn và sample, sample phải thỏa mãn crn
    for crn_id in rule.criterion_ids:
        criterion = Criterion(crn_id)
        criterion.load()
        
        name = criterion.variable_name
        value_sample = sample[vtoi[name]]

        variable = Variable(name)
        variable.load()
        lowerbound, upperbound = variable.lowerbound, variable.upperbound

        # if not check_active_var(sample, vtoi, name):
        #     continue
        
        if variable.type == "numerical":
            value_sample = float(value_sample)
            value = criterion.value
            operator = criterion.operator
            # bound: ["[", "1", "3", ")"]
            if name not in bound:
                bound[name] = ["[", lowerbound, upperbound, "]"]

            if value_sample > value:
                # TODO: might bug here if = happend
                bound[name][1] = value if (value_sample - bound[name][1]) > (value_sample - value) else bound[name][1]
                bound[name][0] = "(" if "=" in operator else "["

            elif value_sample < value:
                bound[name][2] = value if (bound[name][2] - value_sample) > (value - value_sample) else bound[name][2]
                bound[name][-1] = ")" if "=" in operator else "]"
                
            else:  # BUG: sample in uniform distribution
                bound[name][0] = "[" 
                bound[name][1] = value 
                bound[name][2] = value
                bound[name][3] = "]"
        else:
            value_sample = sample[vtoi[name]]
            if name not in bound:
                bound[name] = []

            if value_sample not in bound[name]:
                bound[name].append(value_sample)
                
        if not criterion.check(value_sample):
            return False
    return True


def check_gap(output_name, rule_ids, n_samples=10_000):
    set_values = {}
    variables = set()
    for rule_id in rule_ids:
        rule = Rule(rule_id)
        rule.load()
        for crn_id in rule.criterion_ids:
            crn = Criterion(crn_id)
            crn.load()
            name = crn.variable_name

            variables.add(name)
            
            variable = Variable(name)
            variable.load()
            if variable.type == "numerical":
                if name not in set_values:
                    set_values[name] = set()
                set_values[name].add(crn.value)

    variables = list(variables)
    n_vars = len(variables)
    itov = {i: e for i, e in enumerate(variables)}   # index to var_id
    vtoi = {v: k for k, v in itov.items()}           # var_id to index
    
    all_samples = np.empty((n_samples, n_vars), dtype=np.dtype('U200'))

    for name in variables:
        variable = Variable(name)
        variable.load()

        if variable.type == 'numerical':
            lowerbound, upperbound = variable.lowerbound, variable.upperbound
            values = list(set_values[name])
            all_samples[:, vtoi[name]] = random_by_idx(lowerbound, upperbound, values, n_samples)
        else:
            pool = variable.options
            all_samples[:, vtoi[name]] = np.random.choice(pool, size=n_samples)

    watch = []
    for sample in tqdm(all_samples):
        # if not check_constraint_gen(sample, vtoi): continue
        # check through all rules to see anything match
        bound = {}
        for rule_id in rule_ids:
            if hit_in_rule(vtoi, sample, rule_id, bound): # hit a rule
                break
        else: # if the sample does not hit any rule, then it is a gap
            if bound not in watch:
                watch.append(bound)

    results = []
    for i in range(len(watch)):
        script_gap = []
        for name in watch[i]:
            variable = Variable(name)
            variable.load()

            if variable.type == "numerical":
                formula = watch[i][name]
                condi1 = config.MAPPING_IN_RANGE[formula[0]]
                thrs1 = formula[1]
                thrs2 = formula[2]
                condi2 = config.MAPPING_IN_RANGE[formula[-1]]
                if thrs1 == thrs2:
                    script_gap.append(f'{name} = {thrs1}')
                else:
                    script_gap.append(f'{name} {condi1} {thrs1} AND {name} {condi2} {thrs2}')
            else:
                formula = watch[i][name]
                script_gap.append(f'{name} in {formula}')
        script_gap = '<br>'.join(script_gap)
        
        results.append({'script': script_gap, "output_name": output_name})
    return results


import re
def detect_output_type(value):
    # Define a regular expression for detecting variable names and numbers
    variable_name_pattern = re.compile(r'\b[A-Z_]+\b')
    number_pattern = re.compile(r'\b\d+(\.\d+)?\b')
    
    # Check for variable names or numbers in the value
    if variable_name_pattern.search(value) or number_pattern.search(value):
        return "numerical"
    
    # If no variable names or numbers are found, it is a categorical variable
    return "categorical"
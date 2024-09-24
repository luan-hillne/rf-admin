from typing import Literal, Sequence, List, Tuple, Dict, Set, Union
from src.rtyping import Criterion, Rule, Rules, OutputVariable, OutputValue, Variable, Operator, Value
from src import rtoken


def split_three(crn: Union[Criterion]) -> Tuple[Variable, Operator, Value]:

    idx1 = crn.find(' ')
    idx2 = crn.find(' ', idx1 + 1)

    if "not in" in crn:
        idx2 = crn.find(' ', idx2 + 1)
    return (crn[:idx1], crn[idx1+1 : idx2], crn[idx2+1:])

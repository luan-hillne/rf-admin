from typing import TypeVar

Rule =  TypeVar("Rule", bound=str)
Rules = TypeVar("Rules", bound=str)
Criterion = TypeVar("Criterion", bound=str)

OutputVariable = TypeVar("OutputVariable", bound=str)
OutputValue = TypeVar("OutputValue", bound=str)

Variable = TypeVar("Variable", bound=str)
Operator = TypeVar("Operator", bound=str)
Value = TypeVar("Value", bound=str)
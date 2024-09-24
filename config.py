PREFIX_RULE = "R_"
PREFIX_PARAGRAPH = "P_"
PREFIX_CRITERION = "C_"
PREFIX_EXAMPLE = "E_"
PREFIX_EXAMPLE_VARIABLE = "EV_"
KEY_LENGTH = 5



### SCRIPT
LANGUAGE_TO_SCRIPT_IF = {
    "python": "if",
    "java": "if",
    "sql": "case when"
}

LANGUAGE_TO_SCRIPT_AND = {
    "python": " and ",
    "java": " && ",
    "sql": " and "
}

LANGUAGE_TO_FILE_FORMAT = {
    "python": "py",
    "java": "java",
    "sql": "sql"
}

MAPPING_IN_RANGE = {
    "(": ">",
    ")": "<",
    "[": ">=",
    "]": "<="
}

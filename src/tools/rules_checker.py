from typing import Dict, Union, Tuple
from src.tools.tools import BaseTool
from src.rtyping import Rules, Variable, OutputVariable, Criterion, Operator, Value
from src import rtoken
from src.tools.tools import BaseTool
from src.database_client.vector_database import QDrantVectorDatabase
from src import rtoken, helpers
from src.rtyping import Rules, Variable
from src.llms.chat_models import ChatAzureOpenAI
from abc import ABC, abstractmethod
from typing import Set, Dict, Union
import re


class RulesFormatCheckerTool(BaseTool):
    name: str = "rules_format_checker_tool"
    description: str = ("Use this tool to double check if your generated rules is correct before giving final answer. "
                        "Always use this tool after generated rules."
                        )

    def _is_valid_variable(self, var: Union[Variable, OutputVariable]) -> bool:
        pattern = re.compile(r'^[A-Z0-9_]+$')
        return bool(pattern.match(var))
    
    def _split_three(self, crn: Union[Criterion]) -> Tuple[Variable, Operator, Value]:

        idx1 = crn.find(' ')
        idx2 = crn.find(' ', idx1 + 1)

        if "not in" in crn:
            idx2 = crn.find(' ', idx2 + 1)
        return (crn[:idx1], crn[idx1+1 : idx2], crn[idx2+1:])


    def run(self, query: Rules) -> str:
        if (rtoken.EOR_TOKEN not in query):
            return f"Error: Rules definition is incorrect, missing {rtoken.EOR_TOKEN} to signify the end of a rule"
        
        rules = query.strip().split(rtoken.EOR_TOKEN)

        for rule in rules:
            if rule.strip() == "":
                continue
            
            if rtoken.THN_TOKEN not in rule:
                return f'''Error in rule definition "{rule.strip()}": Missing {rtoken.THN_TOKEN} to mark the conclusion of conditions and the beginning of the output variable with its corresponding value'''
            
            if "or" in rule:
                return f'''Error in rule definition "{rule.strip()}": Invalid token <or>, special token must be <and>, <thn>, <eor>. Try to generate rule without token <or>, <else>'''
            
            if "else" in rule:
                return f'''Error in rule definition "{rule.strip()}": Invalid token <else>, special token must be <and>, <thn>, <eor>. Try to generate rule without token <or>, <else>'''


            crns, output = rule.strip().split(rtoken.THN_TOKEN)
            output_var, op, out_value = self._split_three(output.strip())
            
            if op != "=":
                return f'''Error in rule definition "{rule.strip()}", conclusion part "{output.strip()}": The output must use operator "="'''

            if not self._is_valid_variable(output_var):
                return f'''Error in rule definition "{rule.strip()}", conclusion part "{output.strip()}": The output variable format "{output_var}" is incorrect'''

            if rtoken.AND_TOKEN in out_value:
                return f'''Error in rule definition "{rule.strip()}", conclusion part "{output.strip()}": The output value format "{out_value}" is incorrect, output value must be a formula or a value'''

            crns = crns.strip().split(rtoken.AND_TOKEN)

            for crn in crns:
                var, op, value = self._split_three(crn.strip())
                
                if (op not in ["=", ">", "<", ">=", "<=", "in", "not in"]):
                    return f'''Error in rule definition "{rule.strip()}", condition "{crn.strip()}": operator must be one of ["=", ">", "<", ">=", "<=", "in", "not in"]'''

                if (not self._is_valid_variable(var)):
                    return f'''Error in rule definition "{rule.strip()}", condition "{crn.strip()}": Variable format incorrect "{var}"'''

                if (op in ["in", "not in"]):
                    try:
                        value = eval(value)
                    except:
                        return f'''Error in rule definition "{rule.strip()}", condition "{crn.strip()}": operator "in", "not in" must go with a list of value. For numerical values, use one of operators ["=", ">", "<", ">=", "<="] with numerical value.'''
                
                if (op in ["=", ">", "<", ">=", "<="]):
                    # TODO: could be a formula as well
                    try:
                        value = float(value)
                    except:
                        return f'''Error in rule definition "{rule.strip()}", condition "{crn.strip()}": operator "=", ">", "<", ">=", "<=" must go with numerical value. For categorical values, use the "in" or "not in" operators with a list of possible values'''
        
        return "Rules definition is correct"
    


class RulesMetadataCheckerTool(BaseTool):
    name: str = "variables_search_tool"
    description: str = """Use this tool to search for variables in the database. Always use this tool to ensure that the generated rules do not generate new variables or variable values if similar ones already exist in the database."""

    def build_rule_metadata(self, rules: Rules) -> Dict[Variable, Set[Union[str, float]]]:
        rules = rules.strip().split(rtoken.EOR_TOKEN)

        var_to_value: Dict[Variable, Set] = {}

        for rule in rules:
            if rule.strip() == "":
                continue

            crns, output = rule.strip().split(rtoken.THN_TOKEN)

            output_var, _, output_value = helpers.split_three(output.strip())

            if output_var not in var_to_value:
                var_to_value[output_var] = set()

            try: 
                output_value = float(output_value)
                var_to_value[output_var].add(output_value)
            except:
                var_to_value[output_var].update([output_value])


            crns = crns.strip().split(rtoken.AND_TOKEN)

            for crn in crns:

                var, op, value = helpers.split_three(crn.strip())
                if var not in var_to_value:
                    var_to_value[var] = set()

                try: 
                    value = float(value)
                    var_to_value[var].add(value)
                except:
                    value = eval(value)
                    var_to_value[var].update(value)
        
        return var_to_value
    
    @abstractmethod
    def run(self, query: str) -> str:
        pass
        


class QdrantRulesMetadataCheckerTool(RulesMetadataCheckerTool):
    instruction: str = "You are a helpful assistant that help translate English variable name into Vietnamese"

    examples: str = """NATIONALITY: Quốc tịch
CUSTOMER_SEGMENT: Phân khúc khách hàng
PAYMENT_SOURCE: Nguồn thu nhập
PAYMENT_METHOD: Hình thức nhận thu nhập
CURRENT_JOB_DURATION: Thời gian làm việc tại đơn vị hiện tại (không bao gồm thời gian thử việc)
WORK_EXPERIENCE: Kinh nghiệm làm việc của khách hàng
CONTRACT_TERM: Thời hạn hợp đồng lao động
REMAINING_CONTRACT_TERM: Thời hạn hợp đồng lao động còn lại
CUSTOMER_GENDER: Giới tính khách hàng
CUSTOMER_AGE: Tuổi khách hàng
RESIDENT_CITY_COUNTRY: Quốc gia thành phố thường trú
FLAG_SAME_CITY: Chỉ báo nơi cư trú hiện tại và nơi phát sinh phương án vay vốn cùng địa bàn
BRANCH_CITY: Thành phố đơn vị kinh doanh cho vay
DISTANCE_TO_BRANCH_CITY: Khoảng cách tới đơn vị kinh doanh cho vay
SCORECARD_RANK: Xếp hạng tín dụng nội bộ
CUSTOMER_CONDITION: Điều kiện khách hàng
JOB_CONDITION: Điều kiện việc làm khách hàng
RESIDENT_CONDITION: Điều kiện nơi cư trú khách hàng
AGE_CONDITION: Điều kiện độ tuổi khách hàng
SCORECARD_RANK: Điều kiện xếp hạng tín dụng khách hàng"""

    suffix: str = """Adjust variable names and values to ensure alignment with database. If a direct match exists in the database, adjust the new variable to match with variable in database. If a direct match is absent, consider combining similar existing variables to approximate the new variable's intent. Should no suitable matches or combinations be found in the database, retain the new variable as originally specified"""


    def __init__(
            self, 
            db_client: QDrantVectorDatabase, 
            model_client: ChatAzureOpenAI, 
            top_k: int = 3,
            score_threshold: float = 0.6):
        self.db_client = db_client
        self.top_k = top_k
        self.model_client = model_client
        self.score_threshold = score_threshold

    def run(self, query: str) -> str:
        response = ""

        rules_metadata = self.build_rule_metadata(query)

        # print("[DEBUG]", rules_metadata)


        for var_name in rules_metadata:
            # print("[DEBUG]", "Checking var", var_name)
            var_db_metadata = self.db_client.search_exact_key_value(key="variable_name", value=var_name)

            if not var_db_metadata:
                # print("[DEBUG]", "New Var", var_name)
                prompt = f"""Translate the following variable name to Vietnamese, only output translation result and nothing else: {self.examples}\n{var_name}:"""
                var_desc = self.model_client.generate(self.instruction, prompt, 
                temperature=0.2)
                # print("[DEBUG]", "Var Desc", var_desc)

                values = list(rules_metadata[var_name])

                results = self.db_client.search(f"{var_desc} {values}", top_k=self.top_k, score_threshold=self.score_threshold)

                if len(results) == 0:
                    continue
                else:
                    response += f'''Found new variable name "{var_name}" in rules definition, simliar variable in database:\n'''

                    response += f"Top\tName\tDescription\tType\tValues\n"

                    for i, result in enumerate(results, start=1):
                        payload : Dict = result.payload
                        response += f'''{i}\t{payload["variable_name"]}\t{payload["desc"]}\t{payload["variable_type"]}\t{payload["values"]}\n'''
                response += "\n"
            else:
                if var_db_metadata.payload["variable_type"] != "Categorical":
                    continue

                new_value = []
                existed_value = var_db_metadata.payload["values"]
                for value in rules_metadata[var_name]:

                    if value not in existed_value:
                        new_value.append(value)
                # print("[DEBUG]", "New Value", new_value)
                
                if len(new_value) > 0:
                    response += f'''Found new value {new_value} of variable name "{var_name}" in rules definition, variable values in database: {existed_value}\n'''
            # print("-"*50)
                
            
        if len(response) > 0:
            response += self.suffix
        return response

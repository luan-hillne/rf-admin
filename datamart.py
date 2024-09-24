from database_client import RedisVariableClient
from typing import List

class Variable:
    def __init__(self, variable_name):
        self.variable_name = variable_name

    def load(self):
        redis_client = RedisVariableClient()
        data = redis_client.get_variable(self.variable_name)
        if data:
            self.desc = data["desc"]
            self.type = data["type"]
            self.unit = data["unit"]
            self.options = data["options"]
            self.rule_ids: List[str] = data["rule_ids"]
            self.existed = 1
            self.embedding = data["embedding"]
            self.on_system = data["on_system"]
            self.upperbound = data["upperbound"]
            self.lowerbound = data["lowerbound"]
            self.step = data["step"]
            self.locked = data["locked"]
        else:
            self.existed = 0
        return
        
    
    def to_json(self):
        return {
            "variable_name": self.variable_name,
            "desc": self.desc,
            "type": self.type,
            "rule_ids": self.rule_ids,
            "options": self.options,
            "new_options": [],
            "on_system": self.on_system,
            "upperbound": self.upperbound,
            "lowerbound": self.lowerbound,
            "step": self.step,
            "locked": self.locked,
            "unit": self.unit
        }
    
    def update(self):
        data = {
            "desc": self.desc,
            "type": self.type,
            "unit": self.unit,
            "options": self.options,
            "rule_ids": self.rule_ids,
            "embedding": self.embedding,
            "on_system": self.on_system,
            "upperbound": self.upperbound,
            "lowerbound": self.lowerbound,
            "step": self.step,
            "locked": self.locked
        }
        redis_client = RedisVariableClient()
        redis_client.update_variable(self.variable_name, data)
        return



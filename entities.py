from typing import List, Dict, Optional, Union
from src import rtoken
import utils
from database_client import RedisDocumentClient
import json
from datamart import Variable


class Criterion:
    
    rule_id: str
    variable_name: str
    operator: str
    value: Union[str, List]

    def __init__(self, criterion_id):
        self.criterion_id = criterion_id

    def load(self) -> None:
        redis_client = RedisDocumentClient()
        data: dict = redis_client.get_criterion(self.criterion_id)
        self.rule_id: str = data["rule_id"]
        self.variable_name: str = data["variable_name"]
        self.operator: str = data["operator"]
        self.value = data["value"]
        return

    def update(self) -> None:
        redis_client = RedisDocumentClient()
        data = {
            "rule_id": self.rule_id,
            "variable_name": self.variable_name,
            "operator": self.operator,
            "value": self.value
        }   
        redis_client.update_criterion(self.criterion_id, data)

    def delete(self) -> None:
        self.load()

        variable = Variable(self.variable_name)
        variable.load()
        variable.rule_ids.remove(self.rule_id)
        variable.update()

        rule = Rule(self.rule_id)
        rule.load()
        rule.criterion_ids.remove(self.criterion_id)
        rule.update()

        redis_client = RedisDocumentClient()
        redis_client.delete_criterion(self.criterion_id)
        return

    def to_script(self) -> str:
        return f"{self.variable_name} {self.operator} {self.value}"


    def check(self, value_sample):
        if self.operator in ["in", "not in"]:
            if (self.operator == "in") and  (value_sample in self.value):
                return True
            if (self.operator == "not in") and (value_sample not in self.value):
                return True
        else:
            if (self.operator == '<=') and (value_sample <= self.value):
                return True
            
            if (self.operator == '>=') and (value_sample >= self.value):
                return True
            
            if (self.operator == '=') and (value_sample == self.value):
                return True
            
            if (self.operator == '<') and (value_sample < self.value):
                return True
        
            if (self.operator == '>') and (value_sample > self.value):
                return True
        return False
        

class Rule:

    rule_id: str
    document_id: str
    paragraph_id: str
    status: str
    note: str
    author: str
    output_name: str
    output_value: str
    criterion_ids: List[str]
    metadata: dict
    created_at: str
    updated_at: str


    def __init__(self, rule_id):
        self.rule_id = rule_id
        
    def load(self) -> None:
        redis_client = RedisDocumentClient()
        data: dict = redis_client.get_rule(self.rule_id)

        self.document_id = data["document_id"]
        self.note = data["note"]
        self.author = data["author"]
        self.status = data["status"]
        self.output_name = data["output_name"]
        self.output_value = data["output_value"]
        self.criterion_ids = data["criterion_ids"]
        self.metadata = data["metadata"]
        self.paragraph_id = data["paragraph_id"]
        self.created_at = data["created_at"]
        self.updated_at = data["updated_at"]

        # calc on_system, criterions from self.criterions_ids
        criterions = []
        self.on_system = "true"
        for criterion_id in self.criterion_ids:
            # criterion script
            criterion = Criterion(criterion_id)
            criterion.load()
            criterions.append(criterion.to_script())
            # on_system
            variable = Variable(criterion.variable_name)
            variable.load()
            if not variable.existed:
                self.on_system = "false"
                print("WARNING: variable does not exist, investigate!")
            else:
                if not variable.on_system:
                    self.on_system = "false"
        self.criterions = f" {rtoken.AND_TOKEN} ".join(criterions)
        return
    
    def update(self) -> None:
        redis_client = RedisDocumentClient()
        rule_data = {
            "document_id": self.document_id,
            "paragraph_id": self.paragraph_id,
            "status": self.status,
            "note": self.note,
            "author": self.author,
            "output_name": self.output_name,
            "output_value": self.output_value,
            "criterion_ids": self.criterion_ids,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "updated_at": utils.get_curr_dt()
        }
        redis_client.update_rule(self.rule_id, rule_data)
        return
    
    def to_json(self) -> dict:
        return {
            "document_id": self.document_id,
            "paragraph_id": self.paragraph_id,
            "rule_id": self.rule_id,
            "note": self.note,
            "output_name": self.output_name,
            "output_value": self.output_value,
            "criterions": self.criterions,
            "status": self.status,
            "on_system": self.on_system,
            "metadata": self.metadata,
            "criterion_ids": self.criterion_ids,
            "updated_at": self.updated_at
        }

    def delete(self):
        self.load()

        output_variable = Variable(self.output_name)
        output_variable.load()
        output_variable.rule_ids.remove(self.rule_id)
        output_variable.update()
        
        paragraph = Paragraph(self.paragraph_id)
        paragraph.load()
        paragraph.rule_ids.remove(self.rule_id)
        paragraph.update()
        
        criterion_ids = self.criterion_ids.copy()

        for criterion_id in criterion_ids:
            criterion = Criterion(criterion_id)
            criterion.delete()
        
        redis_client = RedisDocumentClient()
        redis_client.delete_rule(self.rule_id)    
        return

        
class Paragraph:
    document_id: str
    paragraph_id: str
    document_replaced_id: str
    document_modified_id: str
    rule_ids: List[str]
    list_variable_name: List[str]
    updated_at: str
    content: str

    def __init__(self, paragraph_id):
        self.paragraph_id = paragraph_id
        
    def load(self) -> None:
        redis_client = RedisDocumentClient()
        data: dict = redis_client.get_paragraph(self.paragraph_id)

        self.document_id = data["document_id"]
        self.document_replaced_id = data["document_replaced_id"]
        self.document_modified_id = data["document_modified_id"]
        self.rule_ids = data["rule_ids"]
        self.list_variable_name = data["list_variable_name"]
        self.content = data["content"]

        self.no_rules = len(self.rule_ids)
        self.no_rules_active = 0
        self.no_rules_system = 0
        self.updated_at = ""
        for rule_id in self.rule_ids:
            rule = Rule(rule_id)
            rule.load()
            self.no_rules_active += int(rule.status == "active")
            self.updated_at = max(self.updated_at, rule.updated_at)
            self.no_rules_system += int(rule.on_system == "true" and rule.status == "active")
        return

    def update(self) -> None:
        redis_client = RedisDocumentClient()
        data = {
            "document_id": self.document_id,
            "document_replaced_id": self.document_replaced_id,
            "document_modified_id": self.document_modified_id,
            "rule_ids": self.rule_ids,
            "list_variable_name": self.list_variable_name,
            "content": self.content
        }
        redis_client.update_paragraph(self.paragraph_id, data)


    def delete(self) -> None:
        self.load()
        
        document = Document(self.document_id)
        document.load()
        document.paragraph_ids.remove(self.paragraph_id)
        document.update()

        rule_ids = self.rule_ids.copy()
        for rule_id in rule_ids:
            rule = Rule(rule_id)
            rule.delete()

        redis_client = RedisDocumentClient()
        redis_client.delete_paragraph(self.paragraph_id)
    
    def to_json(self) -> dict:
        return {
            "document_id": self.document_id,
            "document_replaced_id": self.document_replaced_id,
            "document_modified_id": self.document_modified_id,
            "rule_ids": self.rule_ids,
            "list_variable_name": self.list_variable_name,
            "content": self.content
        }

        
class Document:
    document_id: str
    document_level: str
    document_type: str
    document_status: str
    paragraph_ids: List[str]
    parent: str
    metadata: dict
    updated_at: str


    def __init__(self, document_id):
        self.document_id = document_id
        
    def update(self):
        redis_client = RedisDocumentClient()
        data = {
            "document_level": self.document_level,
            "document_type": self.document_type,
            "document_status": self.document_status,
            "paragraph_ids": self.paragraph_ids,
            "parent": self.parent,
            "metadata": self.metadata
        }
        redis_client.set_document(self.document_id, data)
        return
    

    def update_info(self, document_level, document_type, document_status):
        self.document_level = document_level
        self.document_type = document_type
        self.document_status = document_status
        self.update()
        return

    def load(self) -> None:
        redis_client = RedisDocumentClient()

        data: dict = redis_client.get_document(self.document_id)
        if data:
            self.paragraph_ids = data["paragraph_ids"]
            self.document_level = data["document_level"]
            self.document_type = data["document_type"]
            self.document_status = data["document_status"]
            self.metadata = data["metadata"]
            self.parent = data["parent"]
            self.existed = True
        else:
            # default if not exist
            self.paragraph_ids: List[str] = []
            self.document_level = ""
            self.document_type = ""
            self.document_status = ""
            self.metadata = {}
            self.parent = []
            self.existed = False

        # synthetic from paragraph ids
        self.rule_ids = []
        self.list_variable_name = []
        self.no_rules = 0
        self.no_rules_active = 0
        self.no_rules_system = 0
        self.replaced = []
        self.modified = []
        self.updated_at = ""

        paragraph_ids = self.paragraph_ids.copy()

        for paragraph_id in paragraph_ids:
            paragraph = Paragraph(paragraph_id)
            paragraph.load()
            if paragraph.document_replaced_id != "":
                idx = -1
                for (i, replaced_data) in enumerate(self.replaced):
                    if replaced_data["document_replaced_id"] == paragraph.document_replaced_id:
                        idx = i
                        break
        
                if idx == -1:
                    self.replaced.append({
                        "document_replaced_id": paragraph.document_replaced_id,
                        "paragraph_ids": [paragraph_id],
                        "rule_ids": paragraph.rule_ids,
                        "no_rules": paragraph.no_rules,
                        "no_rules_active": paragraph.no_rules_active,
                        "no_rules_system": paragraph.no_rules_system,
                    })
                else:
                    self.replaced[idx]["paragraph_ids"].append(paragraph_id)
                    self.replaced[idx]["rule_ids"] += paragraph.rule_ids
                    self.replaced[idx]["no_rules"] += paragraph.no_rules
                    self.replaced[idx]["no_rules_active"] += paragraph.no_rules_active
                    self.replaced[idx]["no_rules_system"] += paragraph.no_rules_system

            elif paragraph.document_modified_id != "":
                idx = -1
                for (i, modified_data) in enumerate(self.modified):
                    if modified_data["document_modified_id"] == paragraph.document_modified_id:
                        idx = i
                        break
        
                if idx == -1:
                    self.modified.append({
                        "document_modified_id": paragraph.document_modified_id,
                        "paragraph_ids": [paragraph_id],
                        "rule_ids": paragraph.rule_ids,
                        "no_rules": paragraph.no_rules,
                        "no_rules_active": paragraph.no_rules_active,
                        "no_rules_system": paragraph.no_rules_system,
                    })
                else:
                    self.modified[idx]["paragraph_ids"].append(paragraph_id)
                    self.modified[idx]["rule_ids"] += paragraph.rule_ids
                    self.modified[idx]["no_rules"] += paragraph.no_rules
                    self.modified[idx]["no_rules_active"] += paragraph.no_rules_active
                    self.modified[idx]["no_rules_system"] += paragraph.no_rules_system
            self.no_rules += paragraph.no_rules
            self.no_rules_active += paragraph.no_rules_active
            self.no_rules_system += paragraph.no_rules_system
            self.rule_ids += paragraph.rule_ids
            self.list_variable_name += paragraph.list_variable_name
            self.updated_at = max(self.updated_at, paragraph.updated_at)
        self.list_variable_name = list(set(self.list_variable_name))
        return

    
    def to_json(self) -> dict:
        return {
            "document_id": self.document_id,
            "document_level": self.document_level,
            "document_type": self.document_type,
            "document_status": self.document_status,
            "rule_ids": self.rule_ids,
            "list_variable_name": self.list_variable_name,
            "replaced": self.replaced,
            "modified": self.modified,
            "no_rules": self.no_rules,
            "no_rules_active": self.no_rules_active,
            "no_rules_system": self.no_rules_system,
            "metadata": self.metadata,
            "updated_at": self.updated_at
        }


    def delete(self):
        self.load()
        paragraph_ids = self.paragraph_ids.copy()

        for paragraph_id in paragraph_ids:
            paragraph = Paragraph(paragraph_id)
            paragraph.delete()
        
        redis_client = RedisDocumentClient()
        redis_client.delete_document(self.document_id)
        return


    def delete_replace(self, document_replaced_id):
        # TODO: reivew updated at
        self.load()
        paragraph_ids = self.paragraph_ids.copy()
        for paragraph_id in paragraph_ids:
            paragraph = Paragraph(paragraph_id)
            paragraph.load()
            if paragraph.document_replaced_id == document_replaced_id:
                paragraph.delete()
        return
    

    def delete_modify(self, document_modified_id):
        # TODO: reivew updated at
        self.load()
        paragraph_ids = self.paragraph_ids.copy()
        for paragraph_id in paragraph_ids:
            paragraph = Paragraph(paragraph_id)
            paragraph.load()
            if paragraph.document_modified_id == document_modified_id:
                paragraph.delete()
        return
    

class Example:
    def __init__(self, example_id):
        self.example_id = example_id

    def load(self):
        redis_client = RedisDocumentClient()
        data = redis_client.get_example(self.example_id)
        self.paragraph = data["paragraph"]
        self.rules = data["rules"]
        self.embedding = data["embedding"]
        return
  
    
class ExampleVariable:
    def __init__(self, example_variable_id):
        self.example_variable_id = example_variable_id

    def load(self):
        redis_client = RedisDocumentClient()
        data = redis_client.get_example_variable(self.example_variable_id)
        self.paragraph = data["paragraph"]
        self.json_data = data["json_data"]
        self.embedding = data["embedding"]
        return
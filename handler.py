from entities import Document, Rule, Criterion, Example, Paragraph, ExampleVariable
from datamart import Variable
from language_model import ChatGPT
from src.llms.embeddings import AzureOpenAIEmbeddings
from database_client import RedisDocumentClient, RedisVariableClient
from typing import List
from tqdm import tqdm
from src import rtoken
import utils, json, config, helpers
import numpy as np


class ParagraphSearcher:
    '''
    1. init from redis
    2. search k-nn 
    3. add an item
    4. remove an item
    '''
    def __init__(self):
        self.model = AzureOpenAIEmbeddings(
            azure_endpoint="https://gradopenai.openai.azure.com",
            api_version="2024-04-01-preview",
            api_key="cdee399144d74ae2a14b2224e0a3dc76",
            model_name="text-embedding-3-large",
            size=3072
        )
        self.embeddings = []
        self.para = []
        self.rules = []
        
        redis_client = RedisDocumentClient()
        example_ids = redis_client.get_all_example_ids()
        for ex_id in example_ids:
            example = Example(ex_id)
            example.load()
            self.embeddings.append(example.embedding)
            self.para.append(example.paragraph)
            self.rules.append(example.rules)
    

    def get_relevant_rules(self, query, return_scores=False, top_k=1):
        if len(self.embeddings) == 0:
            if return_scores:
                return [], []
            else:
                return []

        query_embedding = np.array(self.model.embed_query(query))
        item_embeddings = np.array(self.embeddings)

        # Calculate similariy
        dot_products = query_embedding @ item_embeddings.T
        results, result_scores = [], []
        scores = dot_products/(np.linalg.norm(query_embedding, 2)*np.linalg.norm(item_embeddings, 2, axis=1))
        for i in (scores).argsort()[::-1][:top_k]:
            results.append({
                "paragraph": self.para[i],
                "rules": self.rules[i]
            })
            result_scores.append(scores[i])
        
        if return_scores:
            return results, result_scores
        else:
            return results
    

class ParagraphVariableSearcher:
    '''
    1. init from redis
    2. search k-nn 
    3. add an item
    4. remove an item
    '''
    def __init__(self):
        self.model = AzureOpenAIEmbeddings(
            azure_endpoint="https://gradopenai.openai.azure.com",
            api_version="2024-04-01-preview",
            api_key="cdee399144d74ae2a14b2224e0a3dc76",
            model_name="text-embedding-3-large",
            size=3072
        )

        self.embeddings = []
        self.para = []
        self.json_data = []
        
        redis_client = RedisDocumentClient()
        example_variable_ids = redis_client.get_all_example_variable_ids()
        for ex_id in example_variable_ids:
            example = ExampleVariable(ex_id)
            example.load()
            self.embeddings.append(example.embedding)
            self.para.append(example.paragraph)
            self.json_data.append(example.json_data)
    

    def get_relevant_json_data(self, query, return_scores=False, top_k=1):
        if len(self.embeddings) == 0:
            if return_scores:
                return [], []
            else:
                return []

        query_embedding = np.array(self.model.embed_query(query))
        item_embeddings = np.array(self.embeddings)

        # Calculate similariy
        dot_products = query_embedding @ item_embeddings.T
        results, result_scores = [], []
        scores = dot_products/(np.linalg.norm(query_embedding, 2)*np.linalg.norm(item_embeddings, 2, axis=1))
        for i in (scores).argsort()[::-1][:top_k]:
            results.append({
                "paragraph": self.para[i],
                "json_data": self.json_data[i]
            })
            result_scores.append(scores[i])
        
        if return_scores:
            return results, result_scores
        else:
            return results
    

    
class VariableSearcher:
    '''
    1. init from redis
    2. search k-nn 
    3. add an item
    4. remove an item
    '''
    def __init__(self):
        self.model = AzureOpenAIEmbeddings(
            azure_endpoint="https://gradopenai.openai.azure.com",
            api_version="2024-04-01-preview",
            api_key="cdee399144d74ae2a14b2224e0a3dc76",
            model_name="text-embedding-3-large",
            size=3072
        )
        self.embeddings = []
        self.variables = []
        redis_client = RedisVariableClient()
        all_variable_name = redis_client.get_all_variable_name()
        for name in all_variable_name:
            variable = Variable(name)
            variable.load()
            self.embeddings.append(variable.embedding)
            self.variables.append(variable.variable_name)
    
    def get_relevant_variable(self, query, return_scores=False, top_k=5, score_threshold=0):
        if len(self.embeddings) == 0:
            if return_scores:
                return [], []
            else:
                return []

        query_embedding = np.array(self.model.embed_query(query))
        item_embeddings = np.array(self.embeddings)
        # Calculate similariy
        dot_products = query_embedding @ item_embeddings.T
        results, result_scores = [], []
        scores = dot_products/(np.linalg.norm(query_embedding, 2) * np.linalg.norm(item_embeddings, 2, axis=1))
        for i in (scores).argsort()[::-1][:top_k]:
            if scores[i] >= score_threshold:
                results.append(self.variables[i]); 
                result_scores.append(scores[i])
        
        if return_scores:
            return results, result_scores
        else:
            return results


class Handler:
    def __init__(self):
        self.model = ChatGPT()
        self.variable_searcher = VariableSearcher()
        self.paragraph_variable_searcher = ParagraphVariableSearcher()
        self.paragraph_search = ParagraphSearcher()

    
    def extract_rules(self, content: str) -> List[dict]:
        response = []

        examples = self.paragraph_search.get_relevant_rules(content, top_k=1)
        script = self.model.extract_rules(content, examples)
        rules = script.split(rtoken.EOR_TOKEN)

        for rule in rules:
            if rtoken.THN_TOKEN not in rule:
                continue
            criterions, output = rule.split(rtoken.THN_TOKEN)
            output_name, _, output_value = utils.split_three(output.strip())
            output_value = output_value.replace("'", "")
            criterions = criterions.strip()
            response.append(
                {
                    "output_name": output_name,
                    "output_value": output_value,
                    
                    "criterions": criterions
                }
            )
        return response
   
    
    def save_rule_extraction(self, data) -> str:
        redis_document_client = RedisDocumentClient()
        redis_variable_client = RedisVariableClient()
        
        # handle document
        document_id = data["document_id"]
        document = Document(document_id)
        document.load()

        # create new document if not existed
        if not document.existed:
            document.document_level = data["document_level"]
            document.document_type = data["document_type"]
            document.document_status = data["document_status"]
            document.parent = ""                                    # BUG: should be a list of document_id
            document.update()

        # handle rule from list of json_rules
        json_rules = data["rules"]
        rule_ids = []
        for json_rule in json_rules:
            # parse rule

            list_variable_name = []    # variable <-> rule, criterion, paragraph, document

            # Criterions
            criterions : str = json_rule["criterions"]
            criterions = criterions.split(rtoken.AND_TOKEN)
            
            criterion_ids = []
            for criterion in criterions:
                # parse criterion
                variable_name, operator, value = utils.split_three(criterion.strip())

                # update variable in datamart
                variable = Variable(variable_name)
                variable.load()
                
                if not variable.existed:
                    # create new variable metadata
                    desc = self.model.translate(variable_name)
                    embedding = self.variable_searcher.model.embed_query(desc)
                    
                    if operator in ["in", "not in"]:
                        options = eval(value.replace("'", '"'))
                        variable_data = {
                            "desc": desc,
                            "type": "categorical",
                            "on_system": 1,
                            "unit": "",
                            "options": options,
                            "rule_ids": [],
                            "embedding": embedding,
                            "lowerbound": "",
                            "upperbound": "",
                            "step": "",
                            "locked": False              # allow to update later
                        }
                    else:
                        value = float(value)
                        variable_data = {
                            "desc": desc,
                            "type": "numerical",
                            "on_system": 1,
                            "unit": "",
                            "options": [],
                            "rule_ids": [],
                            "embedding": embedding,
                            "lowerbound": 0,
                            "upperbound": value + 1,
                            "step": 1,
                            "locked": False              # allow to update later
                        }
                    # save variable to redis
                    redis_variable_client.set_variable(variable_name, variable_data)

                    # update variable_searcher
                    self.variable_searcher.embeddings.append(embedding)
                    self.variable_searcher.variables.append(variable_name)
                else:
                    # update current variable metadata
                    if operator in ["in", "not in"]:
                        options = eval(value.replace("'", '"'))
                        variable.options = list(set(variable.options + options))
                        variable.update()
                    else:
                        if not variable.locked:
                            value = float(value)
                            variable.upperbound = max(1 + value, variable.upperbound)
                            variable.update()
                        
                list_variable_name.append(variable_name)
                
                # create new criterion
                if operator in ["in", "not in"]:
                    value_crn = eval(value.replace("'", '"'))
                else:
                    value_crn = float(value)
                criterion_data = {
                    "rule_id": "",                      # placeholder later
                    "variable_name": variable_name,     # bind criterion -> variable
                    "operator": operator,               
                    "value": value_crn,
                    "created_at": utils.get_curr_dt()
                }
                    
                criterion_id = redis_document_client.set_criterion(criterion_data)
                criterion_ids.append(criterion_id)

            # Output
            output_name = json_rule["output_name"]
            output_value = json_rule["output_value"]
            output_variable = Variable(variable_name=output_name)
            output_variable.load()

            if not output_variable.existed:
                output_type = helpers.detect_output_type(output_value)
                desc = self.model.translate(output_name)
                embedding = self.variable_searcher.model.embed_query(desc)
                
                if output_type == "categorical":
                    output_data = {
                        "desc": desc,
                        "type": "categorical",
                        "on_system": 1,
                        "unit": "",
                        "options": [output_value],
                        "rule_ids": [],                 # placeholder
                        "embedding": embedding,
                        "lowerbound": "",
                        "upperbound": "",
                        "step": "",
                        "locked": False
                    }
                elif output_type == "numerical":
                    output_data = {
                        "desc": desc,
                        "type": "numerical",
                        "on_system": 1,
                        "unit": "",
                        "options": [],
                        "rule_ids": [],                 # placeholder
                        "embedding": embedding,
                        "lowerbound": 0,
                        "upperbound": 100,              # in case value is a formula
                        "step": 1,
                        "locked": False
                    }

                # save variable to redis
                redis_variable_client.set_variable(output_name, output_data)
                
                # update variable_searcher
                self.variable_searcher.embeddings.append(embedding)
                self.variable_searcher.variables.append(output_name)
            else:
                if output_variable.type == "categorical":
                    output_variable.options = list(set(output_variable.options + [output_value]))
                    output_variable.update()

            list_variable_name.append(output_name)

            # Rule data
            rule_data = {
                "document_id": document_id,
                "note": data["note"],
                "author": "NhoPV",                      # BUG: get from session later
                "status": document.document_status,
                "output_name": output_name,
                "output_value": output_value,
                "criterion_ids": criterion_ids,         # bind rule -> criterions
                "paragraph_id": "",                     # placeholder
                "metadata": {},
                "created_at": utils.get_curr_dt(),
                "updated_at": utils.get_curr_dt()
            }
            # save rule to redis
            rule_id = redis_document_client.set_rule(rule_data)
            rule_ids.append(rule_id)
            
            # bind criterion <-> rule
            for criterion_id in criterion_ids:
                criterion = Criterion(criterion_id)
                criterion.load()
                criterion.rule_id = rule_id
                criterion.update()
            
            # bind variable <-> rule
            for name in list_variable_name:
                variable = Variable(name)
                variable.load()
                if rule_id not in variable.rule_ids:
                    variable.rule_ids.append(rule_id)
                variable.update()

        # inactive rule ids modified
        document_modified_id: str = data["document_modified_id"]

        # ignore if document_status is draft
        if (document.document_status == "active") and (document_modified_id.strip() != ""):
            document_modified = Document(document_modified_id)
            document_modified.load()
            document_modified.parent = document_id
            document_modified.update()

            rule_ids_modified = data["rule_ids_modified"]
            for rule_id in rule_ids_modified:
                rule = Rule(rule_id)
                rule.load()
                rule.status = "inactive"
                rule.update()

        # inactive document id replaced
        document_replaced_id: str = data["document_replaced_id"]
        
         # ignore if document_status is draft
        if (document.document_status == "active") and (document_replaced_id.strip() != ""):  
            document_replaced = Document(document_replaced_id)
            document_replaced.load()
            document_replaced.document_status = "inactive"
            document_replaced.parent = document_id
            document_replaced.update()
        
            for rule_id in document_replaced.rule_ids:
                rule = Rule(rule_id)
                rule.load()
                rule.status = "inactive"
                rule.update()

        # handle paragraph
        paragraph_data = {
            "document_id": document_id,                         # bind paragraph <-> document
            "document_replaced_id": document_replaced_id,
            "document_modified_id": document_modified_id,
            "content": data["paragraph"],
            "note": data["note"],
            "author": "NhoPV",
            "rule_ids": rule_ids,                               # bind paragraph <-> rules
            "list_variable_name": list_variable_name,           # bind paragraph <-> variables
            "created_at": utils.get_curr_dt()
        }
        
        paragraph_id = redis_document_client.set_paragraph(paragraph_data)
        for rule_id in rule_ids:
            rule = Rule(rule_id)
            rule.load()
            rule.paragraph_id = paragraph_id
            rule.update()

        # bind document -> paragraph
        document.paragraph_ids.append(paragraph_id)
        document.update()
        return "200"


    def get_variables_extraction(self, json_data):
        # this function is called when edit extraction rule, 
        # so that need to get extraction information in case the variable not existed
        response = []
        for data in json_data:
            # data from extraction, could consider as temporary data
            variable_name = data["variable_name"]
            variable_type = data["type"]
            value = data["value"]
            operator = data["operator"]
        
            variable = Variable(variable_name)
            variable.load()

            if variable.existed:
                # variable_data from datamart
                datamart_data = variable.to_json()
                # add data from extraction to client handle later
                datamart_data["value"] = value              
                datamart_data["operator"] = operator
                datamart_data["warning"] = ""
                if datamart_data["type"] == "categorical":
                    datamart_data["new_options"] = list(set(value) - set(datamart_data["options"]))
                    if(len(datamart_data["new_options"]) > 0):
                        datamart_data["warning"] = "yellow"
                response.append(datamart_data)
            else:
                response.append({
                    "variable_name": variable_name,
                    "desc": self.model.translate(variable_name),
                    "type": variable_type,
                    "operator": operator,
                    "rule_ids": [],
                    "options": [],
                    "new_options": value if variable_type == "categorical" else [],
                    "value": value,
                    "warning": "red"
                })
        return response
    

    def extract_variables(self, content: str):
        examples = self.paragraph_variable_searcher.get_relevant_json_data(content, top_k=1)
        script = self.model.extract_variables(content, examples)
        json_variables = json.loads(script)
        response = []
        for json_data in json_variables:
            variable_name = json_data["variable_name"]
            variable = Variable(variable_name)
            variable.load()
            if variable.existed:
                # existed variable

                # variable_data from datamart
                datamart_data = variable.to_json()

                # add data from extraction to client handle later                
                datamart_data["warning"] = ""

                if datamart_data["type"] == "categorical":
                    # check new options
                    datamart_data["new_options"] = list(set(json_data["options"]) - set(datamart_data["options"]))

                    # if new options existed then set warning to yellow
                    if(len(datamart_data["new_options"]) > 0):
                        datamart_data["warning"] = "yellow"
                        
                response.append(datamart_data)
            else:
                variable_type = json_data["type"]
                desc = json_data["desc"]
                if variable_type == "categorical":
                    response.append({
                        "variable_name": variable_name,
                        "desc": desc,
                        "type": variable_type,
                        "rule_ids": [],
                        "options": [],
                        "new_options": json_data["options"],
                        "on_system": 1,
                        "lowerbound": "",
                        "upperbound": "",
                        "step": "",
                        "unit": "",
                        "warning": "red"
                    })
                elif variable_type == "numerical":
                    response.append({
                        "variable_name": variable_name,
                        "desc": desc,
                        "type": variable_type,
                        "rule_ids": [],
                        "options":[],
                        "new_options": [],
                        "on_system": 1,
                        "lowerbound": 0,
                        "upperbound": 100,
                        "step": 1,
                        "unit": "",
                        "warning": "red"
                    })
                else:
                    response.append({
                        "variable_name": variable_name,
                        "desc": desc,
                        "type": variable_type,
                        "rule_ids": [],
                        "options":[],
                        "new_options": [],
                        "on_system": 1,
                        "lowerbound": "",
                        "upperbound": "",
                        "step": "",
                        "unit": "",
                        "warning": "red"
                    })
        
        return response


    def save_variable_extraction(self, data) -> str:
        redis_document_client = RedisDocumentClient()
        redis_variable_client = RedisVariableClient()
        
        # handle document
        document_id = data["document_id"]

        document = Document(document_id)
        document.load()

        # create new document if not existed
        if not document.existed:
            document.document_level = data["document_level"]
            document.document_type = data["document_type"]
            document.document_status = data["document_status"]
            document.parent = []
            document.update()

        document_status = document.document_status

        variables = data["variables"]
        list_variable_name = []

        for ex_data in variables:
            variable_name = ex_data["variable_name"]
            variable_type = ex_data["type"]
            desc = ex_data["desc"]
            embedding = self.variable_searcher.model.embed_query(desc)
            options = list(set(ex_data["options"] + ex_data["new_options"]))
            lowerbound, upperbound, step = ex_data["lowerbound"], ex_data["upperbound"], ex_data["step"]
            unit = ex_data["unit"]

            list_variable_name.append(variable_name)


            variable = Variable(variable_name)
            variable.load()
            if not variable.existed:
                variable_data = {
                    "desc": desc,
                    "type": variable_type,
                    "on_system": 1,
                    "unit": unit if variable_type == "numerical" else "",
                    "options": options if variable_type == "categorical" else [],
                    "rule_ids": [],
                    "embedding": embedding,
                    "lowerbound": lowerbound if variable_type == "numerical" else "",
                    "upperbound": upperbound if variable_type == "numerical" else "",
                    "step": step if variable_type == "numerical" else "",
                    "locked": True
                }
                redis_variable_client.set_variable(variable_name, variable_data)
            else:
                variable.desc = desc
                variable.embedding = embedding

                for i in range(len(self.variable_searcher.variables)):
                    if self.variable_searcher.variables[i] == variable_name:
                        self.variable_searcher.embeddings[i] = embedding
                
                if variable.type == "numerical":
                    variable.lowerbound = float(lowerbound)
                    variable.upperbound = float(upperbound)
                    variable.step = float(step)
                    variable.unit = str(unit)
                else:
                    variable.options = ex_data["options"]
                variable.update()
            

        # inactive document id replaced
        document_replaced_id: str = data["document_replaced_id"]
        
         # ignore if document_status is draft
        if (document_status == "active") and (document_replaced_id.strip() != ""):  
            document_replaced = Document(document_replaced_id)
            document_replaced.load()
            document_replaced.document_status = "inactive"
            document_replaced.parent = document_id
            document_replaced.update()
        

        # handle paragraph
        paragraph_data = {
            "document_id": document_id,
            "document_replaced_id": document_replaced_id,
            "document_modified_id": data["document_modified_id"],
            "content": data["paragraph"],
            "note": data["note"],
            "author": "NhoPV",   # TODO: get from login session later
            "rule_ids": [],
            "list_variable_name": list_variable_name,
            "created_at": utils.get_curr_dt()
        }
        
        paragraph_id = redis_document_client.set_paragraph(paragraph_data)
        # save document
        document.paragraph_ids.append(paragraph_id)
        document.update()
        return "200"


    def get_all_document_ids(self) -> List[str]:
        redis_client = RedisDocumentClient()
        return redis_client.get_all_document_ids()
    

    def get_all_documents(self) -> List[dict]:
        response = []
        redis_client = RedisDocumentClient()

        documents = redis_client.get_all_document_ids()
        for document in documents:
            document_id = document["document_id"]
            document = Document(document_id)
            document.load()
            response.append(document.to_json())
        response.sort(key=lambda x: x["updated_at"], reverse=True)
        return response


    def get_document(self, document_id) -> dict:
        document = Document(document_id)
        document.load()
        return document.to_json()
    
    def get_paragraph(self, paragraph_id) -> dict:
        paragraph = Paragraph(paragraph_id)
        paragraph.load()
        return paragraph.to_json()

    
    def get_rules(self, rule_ids) -> dict:
        response = []
        for rule_id in rule_ids:
            rule = Rule(rule_id)
            rule.load()
            response.append(rule.to_json())
        return response


    def get_all_rules(self) -> List[dict]:
        response = []
        redis_client = RedisDocumentClient()

        documents = redis_client.get_all_document_ids()
        for document in documents:
            document_id = document["document_id"]
            document = Document(document_id)
            document.load()
            rule_ids = document.rule_ids
            for rule_id in rule_ids:
                rule = Rule(rule_id)
                rule.load()
                response.append(rule.to_json())
        return response


    def get_top_variables(self, top_k=7):
        redis_client = RedisVariableClient()
        response = []
        list_variable_name = redis_client.get_all_variable_name()

        for variable_name in list_variable_name:
            variable = Variable(variable_name)
            variable.load()
            response.append(variable.to_json())
        response.sort(key=lambda x: len(x["rule_ids"]), reverse=True)
        return response[:top_k]


    


    def search_variable(self, text_search, search_type, top_k=7):
        response = []
        list_variable_name, result_scores = self.variable_searcher.get_relevant_variable(query=text_search, top_k=top_k, return_scores=True, score_threshold=0.4)
        for variable_name in list_variable_name:
            variable = Variable(variable_name)
            variable.load()
            response.append(variable.to_json())
        return response
        

    def get_all_variables(self):
        redis_client = RedisVariableClient()
        response = []
        list_variable_name = redis_client.get_all_variable_name()

        for variable_name in list_variable_name:
            variable = Variable(variable_name)
            variable.load()
            response.append(variable.to_json())
        response.sort(key=lambda x: len(x["rule_ids"]), reverse=True)
        return response
    

    def get_variables(self, list_variable_name):
        response = []

        for variable_name in list_variable_name:
            variable = Variable(variable_name)
            variable.load()
            response.append(variable.to_json())
        response.sort(key=lambda x: len(x["rule_ids"]), reverse=True)
        return response
    

    def update_variables_on_system(self, system_variables, manual_variables):
        for name in system_variables:
            variable = Variable(name)
            variable.load()
            variable.on_system = 1
            variable.update()

        for name in manual_variables:
            variable = Variable(name)
            variable.load()
            variable.on_system = 0
            variable.update()

        return "200"
    

    def update_rule(self, rule_id, rule_data):
        redis_document_client = RedisDocumentClient()

        rule = Rule(rule_id)
        # Step 1: Load current rule data
        rule.load()
        # Step 2: Delete current criterions
        for criterion_id in rule.criterion_ids:
            criterion = Criterion(criterion_id)
            criterion.delete()
        rule.criterion_ids.clear()
        # Step 3: Update new data
        rule.output_name = rule_data["output_name"]
        rule.output_value = rule_data["output_value"]
        
        criterions = rule_data["criterions"].split(rtoken.AND_TOKEN)
        for criterion in criterions:
            variable_name, operator, value = utils.split_three(criterion.strip())

            if operator in ["in", "not in"]:
                value = eval(value.replace("'", '"'))
            else:
                value = float(value)

            criterion_data = {
                "rule_id": rule_id,
                "variable_name": variable_name,
                "operator": operator,
                "value": value,
                "created_at": utils.get_curr_dt()
            }

            variable = Variable(variable_name)
            variable.load()
            variable.rule_ids.append(rule_id)
            variable.update()
            
            criterion_id = redis_document_client.set_criterion(criterion_data)
            rule.criterion_ids.append(criterion_id)
        rule.update()
        return "200"
    

    def unbind_variable(self, variable_name, document_id):
        variable = Variable(variable_name)
        variable.load()
        if len(variable.rule_ids) != 0:
            return "405"
        else:
            document = Document(document_id)
            document.load()

            for paragraph_id in document.paragraph_ids:
                paragraph = Paragraph(paragraph_id)
                paragraph.load()
                if variable_name in paragraph.list_variable_name:
                    paragraph.list_variable_name.remove(variable_name)
                    paragraph.update()
            return "200"


    def delete_rule(self, rule_id):
        rule = Rule(rule_id)
        rule.delete()
        return "200"
    

    def delete_document(self, document_id):
        document = Document(document_id)
        document.delete()
        return "200"
    

    def delete_document_replace(self, document_id, document_replaced_id):
        document = Document(document_id)
        document.delete_replace(document_replaced_id)
        return "200"
    

    def delete_document_modify(self, document_id, document_modified_id):
        document = Document(document_id)
        document.delete_modify(document_modified_id)
        return "200"


    def gen_code(self, rule_ids, code_format):
        code = ''
        for rule_id in rule_ids:
            rule = Rule(rule_id)
            rule.load()
            if rule.on_system == "false": continue
            code += f"{config.LANGUAGE_TO_SCRIPT_IF[code_format]} ("

            statements = []
            for criterion_id in rule.criterion_ids:
                criterion = Criterion(criterion_id)
                criterion.load()

                value = str(criterion.value)
                variable_name = criterion.variable_name
                opertor = criterion.operator

                variable = Variable(variable_name)
                variable.load()
                if variable.type == "categorical":
                    if code_format == "java":
                        value = value[1:-1].replace("'", '"')
                        if opertor == "in": 
                            statements.append(f"(Arrays.asList({value}).contains({variable_name}))")
                        else:
                            statements.append(f"(!Arrays.asList({value}).contains({variable_name}))")
                    elif code_format == "sql":
                        statements.append(f"({variable_name} {opertor} ({value[1:-1]}))")
                    else:
                        statements.append(f"({variable_name} {opertor} {value})")
                else:
                    statements.append(f"({variable_name} {opertor} {value})")

            code += config.LANGUAGE_TO_SCRIPT_AND[code_format].join(statements) + ")"
            if code_format == "python": code += ":\n\t"
            elif code_format == "java": code += "{\n\t"
            elif code_format == "sql": code += " then "

            code += rule.output_name + " = "
            if code_format == "java":
                code += f'"{rule.output_value}"'
            else:
                code += f"'{rule.output_value}'"

            if code_format == "python": code += "\n\n"
            elif code_format == "sql": code += " end,\n\n"
            elif code_format == "java": code += ";\n}\n\n"
        
        import os
        if os.path.exists(f'data/tmp_code/code.{config.LANGUAGE_TO_FILE_FORMAT[code_format]}'):
            os.remove(f'data/tmp_code/code.{config.LANGUAGE_TO_FILE_FORMAT[code_format]}')
        fout = open(f'data/tmp_code/code.{config.LANGUAGE_TO_FILE_FORMAT[code_format]}', 'w', encoding="utf-8")
        fout.write(code)
        fout.close()
        return


    def get_all_output_name(self):
        response = set()
        redis_client = RedisDocumentClient()

        documents = redis_client.get_all_document_ids()
        for document in documents:
            document_id = document["document_id"]
            document = Document(document_id)
            document.load()
            rule_ids = document.rule_ids
            for rule_id in rule_ids:
                rule = Rule(rule_id)
                rule.load()
                response.add(rule.output_name)
        response = list(response)
        return response


    def check_conflict(self, active_document_ids, draft_document_ids, list_output_name) -> List[dict]:
        document_ids = active_document_ids + draft_document_ids

        rule_ids = []
        for document_id in document_ids:
            document = Document(document_id)
            
            document.load()
            for rule_id in document.rule_ids:
                rule = Rule(rule_id)
                rule.load()
                if rule.output_name in list_output_name:
                    rule_ids.append(rule_id)
        
        rule_ids.sort()
        n_rules = len(rule_ids)

        response = []
        if n_rules < 2:
            return response

        for i in tqdm(range(n_rules - 1)):
            rule1 = Rule(rule_ids[i])
            rule1.load()
            if rule1.status == "inactive": continue

            for j in range(i + 1, n_rules):
                rule2 = Rule(rule_ids[j])
                rule2.load()
                if rule2.status == "inactive": continue
                if rule1.output_name != rule2.output_name: continue

                results = helpers.check_conflict_pair(rule1, rule2)
                if results is not None:
                    if rule1.output_value == rule2.output_value:
                        conflict_type = "Hệ thống"
                    else:
                        conflict_type = "Logic"
                    script = []
                    for (name, overlap) in results:
                        variable = Variable(name)
                        variable.load()

                        if variable.type == "numerical":
                            script.append(f"{overlap[0]} <= {name} <= {overlap[1]}")
                        else:
                            script.append(f"{name} in {list(overlap)}")
                    response.append({
                        "document_id1": rule1.document_id,
                        "document_id2": rule2.document_id,
                        "rule_id1": rule1.rule_id,
                        "rule_id2": rule2.rule_id,
                        "conflict_type": conflict_type,
                        "script": script,
                        "output_name": rule1.output_name
                    })
        return response

    
    def check_gap(self, active_document_ids, draft_document_ids, list_output_name):
        response = []
        document_ids = active_document_ids + draft_document_ids

        groups = {output_name: [] for output_name in list_output_name}
        for document_id in document_ids:
            document = Document(document_id)
            if "OCB" in document_id:
                continue
            if "LPB" in document_id:
                continue
            document.load()
            rule_ids = document.rule_ids

            for rule_id in rule_ids:
                rule = Rule(rule_id)
                rule.load()
                if rule.output_name in groups:
                    groups[rule.output_name].append(rule.rule_id)
        
        n_samples = 500
        for output_name, rule_ids in groups.items():
            if len(rule_ids) == 0:
                continue
            results = helpers.check_gap(output_name, rule_ids, n_samples)
            response.extend(results)
        return response

            
    def init_variable(self, data):
        redis_variable_client = RedisVariableClient()
        variable_name = data["variable_name"]
        variable = Variable(variable_name)
        variable.load()
        if variable.existed:
            return "405"
        else:
            embedding = self.variable_searcher.model.embed_query(data["desc"])
            variable_data = {
                "desc": data["desc"],
                "type": data["type"],
                "on_system": 0,
                "unit": data["unit"],
                "options": data["options"],
                "rule_ids": [],
                "embedding": embedding,
                "lowerbound": data["lowerbound"],
                "upperbound": data["upperbound"],
                "step": data["step"],
                "locked": True
            }
            redis_variable_client.set_variable(variable_name, variable_data)
            self.variable_searcher.embeddings.append(embedding)
            self.variable_searcher.variables.append(variable_name)
            return "200"

    def update_variable(self, data):
        variable_name = data["variable_name"]
        variable = Variable(variable_name)
        variable.load()
        variable.desc = data["desc"]
        embedding = self.variable_searcher.model.embed_query(data["desc"])
        variable.embedding = embedding
        # update new embedding in searcher
        for i in range(len(self.variable_searcher.variables)):
            if self.variable_searcher.variables[i] == variable_name:
                self.variable_searcher.embeddings[i] = embedding

        if variable.type == "numerical":
            variable.lowerbound = float(data["lowerbound"])
            variable.upperbound = float(data["upperbound"])
            variable.step = float(data["step"])
        else:
            variable.options = data["options"]
        variable.unit = data["unit"]
        variable.update()
        return "200"







        

from redis import Redis
import json, os
from dotenv import load_dotenv
import config
from typing import List

load_dotenv()


class DocumentDatabaseClient:
    def __init__(self):
        pass

    def set_document(self, document_id: str, data: dict) -> None:
        raise NotImplementedError()
    
    def get_document(self, document_id: str):
        raise NotImplementedError()
    
    def get_all_document_ids(self) -> List[str]:
        raise NotImplementedError()

    def set_paragraph(self, data: dict) -> str:
        raise NotImplementedError()

    def get_paragraph(self, paragraph_id: str):
        raise NotImplementedError()
    
    def set_rule(self, data: dict) -> str:
        raise NotImplementedError()
    
    def get_rule(self, rule_id: str):
        raise NotImplementedError()
    
    def set_criterion(self, data: dict) -> str:
        raise NotImplementedError()
    
    def get_criterion(self, criterion_id: str):
        raise NotImplementedError()


class RedisDocumentClient(DocumentDatabaseClient):
    DOCUMENT_DATABASE_IDX = 0
    PARAGRAPH_DATABASE_IDX = 1
    RULE_DATABASE_IDX = 2
    CRITERION_DATABASE_IDX = 3
    EXAMPLE_DATABASE_IDX = 4
    EXAMPLE_VARIABLE_DATABASE_IDX = 5
    


    def __init__(self):
        self.host = os.getenv("REDIS_DOCUMENT_HOST")
        self.port = os.getenv("REDIS_DOCUMENT_PORT")
        self.connections = {}
    
    def _get_connection(self, db) -> Redis:
        if db not in self.connections:
            self.connections[db] = Redis(host=self.host, port=self.port, db=db)
        return self.connections[db]
    
    def _get_max_offset(self, db) -> int:
        assert db != self.DOCUMENT_DATABASE_IDX, "Can not get max offset from document database"
            
        connection = self._get_connection(db)
        keys: List[bytes] = connection.keys()
        if len(keys) == 0:
            return 0
        offset = max(keys, key=lambda x: int(x.decode().split("_")[-1]))

        return int(offset.decode().split("_")[-1])
    
    def set_document(self, document_id: str, data: dict) -> str:
        connection = self._get_connection(self.DOCUMENT_DATABASE_IDX)
        connection.set(document_id, json.dumps(data))
        return    

    def get_document(self, document_id: str):
        connection = self._get_connection(self.DOCUMENT_DATABASE_IDX)
        data: bytes = connection.get(document_id)
        if data:
            data = json.loads(data.decode())
        return data 

    def delete_document(self, document_id: str):
        connection = self._get_connection(self.DOCUMENT_DATABASE_IDX)
        connection.delete(document_id)
        return

    def get_all_document_ids(self) -> List[dict]:
        data = []
        connection = self._get_connection(self.DOCUMENT_DATABASE_IDX)
        for key in connection.keys():
            document_id = key.decode()
            document_data = self.get_document(document_id)
            document_data["document_id"] = document_id
            data.append(document_data)
        return data

    def set_paragraph(self, data: dict) -> str:
        offset = self._get_max_offset(self.PARAGRAPH_DATABASE_IDX) + 1
        key = config.PREFIX_PARAGRAPH + (config.KEY_LENGTH - len(str(offset))) * "0" + str(offset)
        connection = self._get_connection(self.PARAGRAPH_DATABASE_IDX)
        connection.set(key, json.dumps(data))
        return key
    
    def get_paragraph(self, paragraph_id: str):
        connection = self._get_connection(self.PARAGRAPH_DATABASE_IDX)
        data: bytes = connection.get(paragraph_id)
        if data:
            data = json.loads(data.decode())
        return data

    def update_paragraph(self, paragraph_id: str, data: dict) -> None:
        connection = self._get_connection(self.PARAGRAPH_DATABASE_IDX)
        connection.set(paragraph_id, json.dumps(data))

    def delete_paragraph(self, paragraph_id: str) -> None:
        connection = self._get_connection(self.PARAGRAPH_DATABASE_IDX)
        connection.delete(paragraph_id)
        return

    def set_rule(self, data: dict) -> str:
        offset = self._get_max_offset(self.RULE_DATABASE_IDX) + 1
        key = config.PREFIX_RULE + (config.KEY_LENGTH - len(str(offset))) * "0" + str(offset)
        connection = self._get_connection(self.RULE_DATABASE_IDX)
        connection.set(key, json.dumps(data))
        return key
    
    def update_rule(self, rule_id: str, data: dict) -> None:
        connection = self._get_connection(self.RULE_DATABASE_IDX)
        connection.set(rule_id, json.dumps(data))
        return

    def get_rule(self, rule_id: str):
        connection = self._get_connection(self.RULE_DATABASE_IDX)
        data: bytes = connection.get(rule_id)
        if data:
            data = json.loads(data.decode())
        return data

    def delete_rule(self, rule_id: str):
        connection = self._get_connection(self.RULE_DATABASE_IDX)
        connection.delete(rule_id)
        return

    def set_criterion(self, data: dict) -> str:
        offset = self._get_max_offset(self.CRITERION_DATABASE_IDX) + 1
        key = config.PREFIX_CRITERION + (config.KEY_LENGTH - len(str(offset))) * "0" + str(offset)
        connection = self._get_connection(self.CRITERION_DATABASE_IDX)
        connection.set(key, json.dumps(data))
        return key
    
    def get_criterion(self, criterion_id: str):
        connection = self._get_connection(self.CRITERION_DATABASE_IDX)
        data: bytes = connection.get(criterion_id)
        if data:
            data = json.loads(data.decode())
        return data

    def delete_criterion(self, criterion_id: str):
        connection = self._get_connection(self.CRITERION_DATABASE_IDX)
        connection.delete(criterion_id)
        return

    def update_criterion(self, criterion_id: str, data: dict):
        connection = self._get_connection(self.CRITERION_DATABASE_IDX)
        connection.set(criterion_id, json.dumps(data))
        return
        
    def set_example(self, data: dict) -> str:
        offset = self._get_max_offset(self.EXAMPLE_DATABASE_IDX) + 1
        key = config.PREFIX_EXAMPLE + (config.KEY_LENGTH - len(str(offset))) * "0" + str(offset)
        connection = self._get_connection(self.EXAMPLE_DATABASE_IDX)
        connection.set(key, json.dumps(data))
        return
    
    def get_example(self, example_id: dict) -> str:
        connection = self._get_connection(self.EXAMPLE_DATABASE_IDX)
        data: bytes = connection.get(example_id)
        if data:
            data = json.loads(data.decode())
        return data
    
    def get_all_example_ids(self) -> List[str]:
        connection = self._get_connection(self.EXAMPLE_DATABASE_IDX)
        keys = [key.decode() for key in connection.keys()]
        return keys
    
    def delete_all_example(self):
        connection = self._get_connection(self.EXAMPLE_DATABASE_IDX)
        connection.flushdb()
        return
    

    def set_example_variable(self, data: dict) -> str:
        offset = self._get_max_offset(self.EXAMPLE_VARIABLE_DATABASE_IDX) + 1
        key = config.PREFIX_EXAMPLE_VARIABLE + (config.KEY_LENGTH - len(str(offset))) * "0" + str(offset)
        connection = self._get_connection(self.EXAMPLE_VARIABLE_DATABASE_IDX)
        connection.set(key, json.dumps(data))

    def get_example_variable(self, example_variable_id: str) -> str:
        connection = self._get_connection(self.EXAMPLE_VARIABLE_DATABASE_IDX)
        data: bytes = connection.get(example_variable_id)
        if data:
            data = json.loads(data.decode())
        return data

    def get_all_example_variable_ids(self) -> List[str]:
        connection = self._get_connection(self.EXAMPLE_VARIABLE_DATABASE_IDX)
        keys = [key.decode() for key in connection.keys()]
        return keys
    
    def delete_all_example_variable(self):
        connection = self._get_connection(self.EXAMPLE_VARIABLE_DATABASE_IDX)
        connection.flushdb()
        return
   


class VariableDatabaseClient:
    def __init__(self):
        pass

    def set_variable(self, variable_name: str, data: dict):
        raise NotImplementedError()

    def get_variable(self, variable_name: str):
        raise NotImplementedError()
    
    def update_variable(self, variable_name: str):
        raise NotImplementedError()


class RedisVariableClient(VariableDatabaseClient):
    def __init__(self):
        self.connection = Redis(host=os.getenv("REDIS_VARIABLE_HOST"), 
                                port=os.getenv("REDIS_VARIABLE_PORT"),
                                db=0)
    
    def set_variable(self, variable_name: str, data: dict) -> None:
        if self.connection.get(variable_name):
            raise SystemError("Existed variable")
        self.connection.set(variable_name, json.dumps(data))
        return

    def get_variable(self, variable_name: str):
        data: bytes = self.connection.get(variable_name)
        if data:
            data = json.loads(data.decode())
        return data 


    def update_variable(self, variable_name: str, data: dict) -> None:
        if self.connection.get(variable_name) is None:
            raise SystemError("Not existed variable")
        self.connection.set(variable_name, json.dumps(data))
        return

    def get_all_variable_name(self):
        data = [key.decode() for key in self.connection.keys()]
        return data


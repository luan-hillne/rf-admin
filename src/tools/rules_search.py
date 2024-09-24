from src.tools.tools import BaseTool
from src.database_client.vector_database import RedisVectorDatabase, QDrantVectorDatabase
from typing import Dict, Union
from abc import ABC, abstractmethod
from typing import List, Any


class RulesSearch(BaseTool):
    name: str = "rules_search_tool"
    description: str = """Use this tool to search rules extracted from paragraph from in database."""

    @abstractmethod
    def run(self, query: str) -> str:
        '''
        '''

class RedisRulesSearchTool(RulesSearch):
    def __init__(self, client: RedisVectorDatabase, para_key: str, rules_key: str, no_examples: int = 2):
        self.client = client
        self.no_examples = no_examples
        self.para_key = para_key
        self.rules_key = rules_key


    def run(self, query: str) -> str:
        results = self.client.search(query, top_k=self.no_examples)
        script = ""
        for i, result in enumerate(results, start=1):
            script += f"Example {i}:\n\nParagraph: {result[self.para_key]}\n\nRules: {result[self.rules_key]}\n\n"
        return script
        

class QdrantRulesSearchTool(RulesSearch):
    def __init__(self, client: QDrantVectorDatabase, para_key: str, rules_key: str, no_examples: int = 2):
        self.client = client
        self.no_examples = no_examples
        self.para_key = para_key
        self.rules_key = rules_key


    def run(self, query: str) -> str:
        results = self.client.search(query, top_k=self.no_examples)
        script = ""
        for i, result in enumerate(results, start=1):
            payload = result.payload
            script += f"Example {i}:\n\nParagraph: {payload[self.para_key]}\n\nRules: {payload[self.rules_key]}\n\n"
        return script
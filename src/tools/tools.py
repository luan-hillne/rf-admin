from typing import Dict

class BaseTool:
    name: str
    description: str 
    
    def run(self, query: str) -> str:
        raise NotImplementedError()

from abc import ABC, abstractmethod
from typing import List
from openai import AzureOpenAI


class ChatAzureOpenAI:
    def __init__(
            self, 
            azure_endpoint: str,
            api_version: str | None = None,
            api_key: str | None = None,
            model_name: str | None = None,
        ):
        
        self.client = AzureOpenAI(
            azure_endpoint=azure_endpoint,
            api_version=api_version,
            api_key=api_key
        )

        self.model_name = model_name


    def generate(self, instruction: str, prompt: str, temperature: float = 0.5) -> str:
        response = self.client.chat.completions.create(
        model=self.model_name,
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt}
        ],
        temperature=temperature).choices[0].message.content
        return response


    def chat(self,):
        pass
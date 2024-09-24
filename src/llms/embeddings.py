from abc import ABC, abstractmethod
from typing import List, Optional
from openai import AzureOpenAI

class Embeddings(ABC):
    """An interface for embedding models.

    This is an interface meant for implementing text embedding models.

    Text embedding models are used to map text to a vector (a point in n-dimensional
    space).

    Texts that are similar will usually be mapped to points that are close to each
    other in this space. The exact details of what's considered "similar" and how
    "distance" is measured in this space are dependent on the specific embedding model.

    This abstraction contains a method for embedding a list of documents and a method
    for embedding a query text. The embedding of a query text is expected to be a single
    vector, while the embedding of a list of documents is expected to be a list of
    vectors.

    Usually the query embedding is identical to the document embedding, but the
    abstraction allows treating them independently.

    In addition to the synchronous methods, this interface also provides asynchronous
    versions of the methods.

    By default, the asynchronous methods are implemented using the synchronous methods;
    however, implementations may choose to override the asynchronous methods with
    an async native implementation for performance reasons.
    """
    size: int

    @abstractmethod
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed search docs."""

    @abstractmethod
    def embed_query(self, text: str) -> List[float]:
        """Embed query text."""


class AzureOpenAIEmbeddings(Embeddings):
    def __init__(
            self, 
            azure_endpoint: str,
            api_version: Optional[str] = None,
            api_key: Optional[str] = None,
            model_name: Optional[str] = None,
            size: Optional[int] = None):
        
        self.client = AzureOpenAI(
            azure_endpoint=azure_endpoint,
            api_version=api_version,
            api_key=api_key
        )

        self.model_name = model_name
        self.size = size

    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [emb.embedding[:self.size] for emb in self.client.embeddings.create(input=texts, model=self.model_name).data]
        

    def embed_query(self, text: str) -> List[float]:
        return self.client.embeddings.create(input=text, model=self.model_name).data[0].embedding[:self.size]
        
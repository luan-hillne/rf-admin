from redis import Redis
from typing import List, Any, Optional, Dict
from sklearn.metrics.pairwise import cosine_similarity
from src.llms.embeddings import Embeddings
from abc import ABC, abstractmethod
from qdrant_client import QdrantClient, models
import numpy as np
import json


class VectorDatabase(ABC):
    embeddings_model: Embeddings 
    
    @abstractmethod
    def search(self, query: str, top_k: int = 5) -> List[Any]:
        '''
        Search for closest vectors in collection by Cosine Similarity
        '''

    @abstractmethod
    def search_exact_key_value(self, key:str, value: str) -> Dict:
        '''
        Determine if the query existed in database
        '''
        

class QDrantVectorDatabase(VectorDatabase):
    def __init__(
            self, 
            host: str,
            port: int,
            collection_name: str,
            embeddings_model: Embeddings,
            https: bool = None):

        self.client = QdrantClient(host=host, port=port, https=https)
        self.collection_name = collection_name
        self.embeddings_model = embeddings_model

    def search(self, query: str, top_k: int = 5, score_threshold: Optional[float] = None) -> List[Any]:
        query_vector = self.embeddings_model.embed_query(query)
        return self.client.search(collection_name=self.collection_name, query_vector=query_vector, score_threshold=score_threshold)[:top_k]

    def search_exact_key_value(self, key:str, value: str) -> Dict:
        records, _ = self.client.scroll(
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=key,
                        match=models.MatchValue(value=value),
                    )
                    
                ]
            ),
        )
        
        if len(records) == 0:
            return None

        assert len(records) == 1, "Found duplicate key - value in database"
        return records[0]

    
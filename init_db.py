import pandas as pd
from database_client import RedisDocumentClient
from src.llms.embeddings import AzureOpenAIEmbeddings


redis_client = RedisDocumentClient()

model = AzureOpenAIEmbeddings(
    azure_endpoint="https://gradopenai.openai.azure.com",
    api_version="2024-04-01-preview",
    api_key="cdee399144d74ae2a14b2224e0a3dc76",
    model_name="text-embedding-3-large",
    size=3072
)


def insert_paragraph_rule():
    df = pd.read_excel("data/rag.xlsx")

    for i, row in df.iterrows():
        paragraph = row["input_text"]
        rules = row["output_rules"]
        embedding = model.embed_query(paragraph)
        data = {
            "paragraph": paragraph,
            "rules": rules,
            "embedding": embedding
        }
        redis_client.set_example(data)
    return

def insert_paragraph_variable():
    df = pd.read_excel("data/rag_variable.xlsx")

    for i, row in df.iterrows():
        paragraph = row["input_text"]
        rules = row["output"]
        embedding = model.embed_query(paragraph)
        data = {
            "paragraph": paragraph,
            "json_data": rules,
            "embedding": embedding
        }
        redis_client.set_example_variable(data)
    return


if __name__ == "__main__":
    redis_client.delete_all_example()
    insert_paragraph_rule()

    redis_client.delete_all_example_variable()
    insert_paragraph_variable()
    
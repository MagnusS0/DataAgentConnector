from functools import lru_cache

from sentence_transformers import SentenceTransformer


@lru_cache(maxsize=4)
def get_embedding_model(
    name: str, device: str, trust_remote_code: bool = True
) -> SentenceTransformer:
    """
    Load and return a SentenceTransformer model.
    """
    model = SentenceTransformer(
        model_name_or_path=name, device=device, trust_remote_code=trust_remote_code
    )
    return model


def generate_embeddings(
    model: SentenceTransformer, docs: list[str] | str, batch_size: int = 32
) -> list[list[float]]:
    """
    Generate embeddings for a list of documents using the provided model.
    """
    if isinstance(docs, str):
        docs = [docs]
    embeddings = model.encode(docs, batch_size=batch_size).tolist()

    return embeddings

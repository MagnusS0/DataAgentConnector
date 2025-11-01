from functools import lru_cache

from sentence_transformers import SentenceTransformer

from app.core.config import get_settings


class EmbeddingGenerator:
    """Domain service for generating embeddings."""

    def __init__(
        self,
        model_name: str | None = None,
        device: str | None = None,
    ):
        settings = get_settings()
        self.model_name = model_name or settings.embedding_model_name
        self.device = device or settings.device
        self._model = None

    @property
    def model(self) -> SentenceTransformer:
        """Lazy-load the embedding model."""
        if self._model is None:
            self._model = _load_model(self.model_name, self.device)
        return self._model

    async def generate_batch(
        self, texts: list[str], batch_size: int = 32
    ) -> list[list[float]]:
        """Generate embeddings for multiple texts."""
        embeddings = self.model.encode(texts, batch_size=batch_size).tolist()
        return embeddings

    async def generate(self, text: str) -> list[float]:
        """Generate embedding for a single text."""
        results = await self.generate_batch([text])
        return results[0]


@lru_cache(maxsize=4)
def _load_model(name: str, device: str) -> SentenceTransformer:
    """Load and cache embedding model."""
    return SentenceTransformer(
        model_name_or_path=name,
        device=device,
        trust_remote_code=True,
    )

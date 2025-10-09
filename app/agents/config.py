from functools import lru_cache
from app.core.config import get_settings
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.models.openai import OpenAIChatModel


@lru_cache()
def get_openai_provider() -> OpenAIProvider:
    """
    Dependency to get the OpenAI provider.
    Caches the provider instance.
    """
    settings = get_settings()

    if not settings.llm_api_key:
        raise ValueError("LLM_API_KEY must be set in environment variables")
    if settings.llm_base_url:
        return OpenAIProvider(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
        )
    return OpenAIProvider(api_key=settings.llm_api_key)


@lru_cache(maxsize=1)
def get_llm_model():
    """
    Dependency to get the LLM model.
    """
    settings = get_settings()
    provider = get_openai_provider()

    if not settings.llm_model_name:
        raise ValueError("LLM_MODEL_NAME must be set in environment variables")

    return OpenAIChatModel(
        model_name=settings.llm_model_name,
        provider=provider,
    )

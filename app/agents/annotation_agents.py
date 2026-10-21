from pydantic_ai import Agent, ModelSettings

from app.agents.config import get_llm_model
from app.agents.prompts.annotation_prompts import (
    PROMPT_DATABASE_ANNOTATION_AGENT,
    PROMPT_TABLE_ANNOTATION_AGENT,
)
from app.core.config import get_settings
from app.schemas.agents import DatabaseDescription, TableDescription

settings = get_settings()

table_annotation_agent = Agent(
    model=get_llm_model(),
    name="Table Annotation Agent",
    model_settings=ModelSettings(max_tokens=512, temperature=0.3),
    system_prompt=f"{PROMPT_TABLE_ANNOTATION_AGENT}",
    output_type=TableDescription,
    retries={"output": 3},
)

database_annotation_agent = Agent(
    model=get_llm_model(),
    name="Database Annotation Agent",
    model_settings=ModelSettings(max_tokens=1024, temperature=0.3),
    system_prompt=f"{PROMPT_DATABASE_ANNOTATION_AGENT}",
    output_type=DatabaseDescription,
    retries={"output": 3},
)

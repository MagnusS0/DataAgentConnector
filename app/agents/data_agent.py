from pydantic_ai import Agent, ModelSettings
from pydantic_ai.mcp import MCPServerStreamableHTTP

from app.core.config import get_settings
from app.agents.config import get_llm_model
from app.agents.prompts.data_agent_prompt import PROMPT_DATA_AGENT

settings = get_settings()

mcp_client = MCPServerStreamableHTTP(url="http://localhost:8001/mcp/", max_retries=3)


data_agent = Agent(
    model=get_llm_model(),
    name="Data Agent",
    instructions=f"{PROMPT_DATA_AGENT}",
    toolsets=[mcp_client],
    model_settings=ModelSettings(max_tokens=16384, temperature=0.3),
    output_type=str,
)

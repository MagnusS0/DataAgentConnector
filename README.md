# Data Agent Connector

This provides a blueprint to go from database connection string to up and running SQL Data Agent in seconds.
Connect to any database that can be used with SQLAlchemy (might need to install specific engine).
Builds search and convenience tools on top of a DB, made for SQL agents to interact with through the MCP protocol.
Plus mirrored REST endpoint that can be connected to UIs etc.

## Key Features

- **Read-only SQL gateway:** SQLAlchemy engines are locked to safe commands defined in
`[tool.dac.settings.allowed_sql_commands]`.
- **Automatic metadata:** LLM agents summarise tables; LanceDB stores summaries plus
sentence-transformer embeddings (embeddings are currently unused).
- **Column-content retrieval:** Distinct textual values are sampled, filtered, and indexed with
LanceDB BM25 for direct content search in columns.
- **MCP + REST:** `/mcp` serves FastMCP, while `/widgets/*` exposes REST endpoints
for UI integration with OpenBB (customize this to your preferred UI).
- **Config-driven:** `databases.toml` declares available data sources, `pyproject.toml` under `[tool.dac.settings]` for runtime settings and `.env` (or
environment variables) configures the LLM provider.


### MCP (Mounted at /mcp)

| Tool | Summary |
| --- | --- |
| get_databases | Lists registered databases and descriptions. |
| show_tables / show_views | Enumerates tables/views with cached annotations where available. |
| describe_table / describe_view | Returns DDL-like metadata or view SQL. |
| get_distinct_values | Pulls sample categorical values (limit enforced). |
| preview_table | Returns first rows of non-binary columns. |
| find_relevant_columns_and_content | BM25 search over distinct textual values with score filtering. |
| query_database | Executes read-only SQL with a configurable row cap (mcp_query_limit). |
| join_path | Suggests shortest join sequences or Steiner-tree paths across tables. |


## Getting Started

1. Clone the repository:

    ```bash
    git clone https://github.com/MagnusS0/DataAgentConnector.git
    cd DataAgentConnector
    ```

2. Install dependencies:

   ```bash
   uv sync --group ai
   ```

3. Configure your databases in `databases.toml`:

   ```toml
   [databases.my_database]
   connection_string = "sqlite:///path/to/your/database.db"
   description = "My local SQLite database"

   [databases.another_database]
   connection_string = "postgresql://user:password@localhost:5432/another_database"
   description = "Another PostgreSQL database"
   ```

4. Set up your LLM provider in `.env`:

   ```env
   LLM_API_KEY=your_api_key_here
   LLM_MODEL_NAME=default-model
   LLM_BASE_URL=https://api.your-llm-provider.com
   ```

5. Run the application:

   ```bash
   uv run uvicorn app.main:app --reload
   ```

## Project Structure

```
DataAgentConnector/
├── app/
│   ├── agents/
│   ├── core/
│   ├── domain/
│   ├── interfaces/
│   ├── models/
│   ├── schemas/
│   ├── repositories/
│   ├── services/
│   └── main.py
├── databases.toml
├── pyproject.toml
├── .env
└── README.md
```


## Indexing & Metadata Pipeline

  1. Column extraction (`app/domain/extract_colum_content.py`) samples distinct textual
     values while filtering binary, numeric, or overly long fields; tunable via
     `tool.dac.settings.fts_extraction_options`.
  2. FTS indexing (`app/services/indexing_service.py`) persists values into LanceDB tables named
     `column_contents_<database>` and builds BM25 indexes.
  3. Annotation workflow (`app/services/annotation_service.py`) runs LLM prompts with table metadata,
     previews, and sampled values (schema hashes used to skip already processed tables), embeddings are added via
     sentence-transformers.

## FK Graph & Join Paths

Foreign key constraints are analyzed to build a cached CSR adjacency matrix (`app/domain/fk_analyzer.py`) where tables are nodes and FKs are edges. For two tables, BFS finds the shortest join sequence. For 3+ tables, an approximate Steiner tree (MST on all-pairs distances) computes the minimal spanning network, returning ordered `JoinStep` objects with FK column mappings.

This allows agents to request optimal join paths across multiple tables when formulating SQL queries.
Even when there is no direct foreign key relationship defined in the database schema.


### Stats for the interested user

Indexing and annotating all of [BIRD-SQL](https://huggingface.co/datasets/birdsql/bird23-train-filtered) training databases (69 databases) results in:
- Table annotations stored successfully in ~200 seconds
- Content FTS indices created successfully in ~5 seconds

> Hardware: Intel i9-14900K, 64GB RAM, RTX 3090 running Menlo/Jan-nano (4B params) using vLLM

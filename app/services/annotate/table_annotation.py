from hashlib import sha256
import asyncio
from collections.abc import Sequence

from sqlalchemy.engine import RowMapping

from app.agents.annotation_agents import table_annotation_agent
from app.db.sql_alchemy import (
    connection_scope,
    list_tables,
    get_table_metadata,
    get_table_preview,
)
from app.models.database_registry import TableMetadata
from app.models.agents import TableDescription
from app.db.lance_db import TableAnnotation, get_lance_db_async, batch_insert_async
from app.services.annotate.embed import get_embedding_model, generate_embeddings
from app.core.config import get_settings

settings = get_settings()


async def annotate_table(
    database: str, table_name: str, lance: bool = False
) -> TableAnnotation | None:
    """Generate an annotation for a specific table using the annotation agent."""
    with connection_scope(database) as conn:
        metadata = get_table_metadata(conn, table_name)
        preview = get_table_preview(conn, table_name, limit=5)

    if lance:
        exists, schema_hash = await _check_existing_annotation(
            database, table_name, metadata
        )
        if exists:
            return None
        else:
            table_description = await _get_description_from_agent(
                database, table_name, metadata, preview
            )

        annotation = TableAnnotation(
            database_name=database,
            table_name=table_name,
            description=table_description,
            metadata_json=metadata.model_dump_json(),
            embeddings=None,  # Placeholder, will be set later
            schema_hash=schema_hash,
        )
        return annotation

    else:
        table_description = await _get_description_from_agent(
            database, table_name, metadata, preview
        )

        return TableAnnotation(
            database_name=database,
            table_name=table_name,
            description=table_description,
            metadata_json=metadata.model_dump_json(),
            embeddings=None,
            schema_hash=None,
        )


async def annotate_database(
    database: str, lance: bool = False
) -> list[TableAnnotation]:
    """Annotate all tables in a given database."""
    with connection_scope(database) as conn:
        tables = list_tables(conn)

    annotations: list[TableAnnotation] = []
    tasks = []
    for table_name in tables:
        tasks.append(annotate_table(database, table_name, lance=lance))

    results = await asyncio.gather(*tasks)
    for result in results:
        if result is not None:
            annotations.append(result)

    return annotations


async def save_to_lance_db(
    annotations: list[TableAnnotation] | TableAnnotation,
) -> None:
    """
    Saves a list of TableAnnotation objects to the LanceDB table.
    Expects the `vector` field to be None and fills it with generated embeddings.
    """
    if isinstance(annotations, TableAnnotation):
        annotations = [annotations]

    model = get_embedding_model(
        settings.embedding_model_name,
        device=settings.device,
    )

    embeddings = generate_embeddings(
        model,
        [annotation.description for annotation in annotations],
        batch_size=32,
    )

    for annotation, vector in zip(annotations, embeddings):
        annotation.embeddings = vector

    lance_db = await get_lance_db_async()
    table = await lance_db.open_table("table_annotations")
    await batch_insert_async(table, annotations)


async def _get_description_from_agent(
    database: str,
    table_name: str,
    metadata: TableMetadata,
    preview: Sequence[RowMapping],
) -> str:
    """Use the annotation agent to generate a table description."""
    table_description = await table_annotation_agent.run(
        f"""
        For the table named '{table_name}' in the database '{database}',
        generate a concise description based on the following metadata and data preview.
        <metadata>
        {metadata.model_dump()}
        </metadata>
        <preview>
        {[dict(row) for row in preview]}
        </preview>
        """
    )
    output = table_description.output
    if not isinstance(output, TableDescription):
        raise RuntimeError("Annotation agent did not return a TableDescription.")
    return output.description


async def _check_existing_annotation(
    database: str, table_name: str, metadata: TableMetadata
) -> tuple[bool, str]:
    """Check if an annotation already exists for the given table based on schema hash."""
    hash_input = f"{database}.{table_name}.{metadata.model_dump_json()}"
    schema_hash = sha256(hash_input.encode("utf-8")).hexdigest()

    lance_db = await get_lance_db_async()
    table = await lance_db.open_table("table_annotations")
    existing = (
        await table.query().where(f"schema_hash == '{schema_hash}'").limit(1).to_list()
    )
    return (len(existing) > 0, schema_hash)

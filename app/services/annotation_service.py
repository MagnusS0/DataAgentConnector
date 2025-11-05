import asyncio
from functools import lru_cache
from typing import TypedDict

from app.agents.annotation_agents import table_annotation_agent
from app.repositories.sql_db import (
    connection_scope,
    list_tables,
    get_table_metadata,
    get_table_preview,
    list_schemas_for,
)
from app.repositories.annotations import AnnotationRepository
from app.repositories.column_contents import ColumnContentRepository
from app.repositories.lance_db import get_lance_db, open_or_create_table
from app.models.lance import TableAnnotation
from app.schemas.agents import TableDescription as TableDescriptionModel
from app.domain.embed import EmbeddingGenerator
from app.core.config import get_settings
from app.core.logging import get_logger

settings = get_settings()
logger = get_logger(__name__)


class TableDescription(TypedDict):
    """Type for table description dictionary."""

    table_name: str
    schema_name: str
    description: str | None


class AnnotationService:
    """Service for generating and storing table annotations."""

    def __init__(
        self,
        annotation_repo: AnnotationRepository,
        embedding_gen: EmbeddingGenerator,
    ):
        self.annotation_repo = annotation_repo
        self.embedding_gen = embedding_gen

    async def annotate_table(
        self,
        database: str,
        schema: str,
        table_name: str,
        *,
        skip_if_exists: bool = False,
    ) -> TableAnnotation | None:
        """Generate annotation for a table using the annotation agent."""
        try:
            with connection_scope(database) as conn:
                metadata = get_table_metadata(conn, table_name, schema=schema)
                preview = get_table_preview(conn, table_name, schema=schema, limit=5)

            logger.debug(
                "Annotating table '%s' (schema '%s') in database '%s'.",
                table_name,
                schema,
                database,
            )

            # Check if already exists
            schema_hash: str | None = None
            if skip_if_exists:
                schema_hash = self.annotation_repo.compute_schema_hash(
                    database, schema, table_name, metadata
                )
                if await self.annotation_repo.exists_by_schema_hash(schema_hash):
                    logger.debug(
                        (
                            "Annotation for table '%s' (schema '%s') in database '%s'"
                            " is up-to-date, skipping."
                        ),
                        table_name,
                        schema,
                        database,
                    )
                    return None

            # Get column content samples
            column_repo = ColumnContentRepository(database, schema=schema)
            column_samples = await self._get_column_samples(
                column_repo, table_name, schema
            )

            description = await self._generate_description(
                database, schema, table_name, metadata, preview, column_samples
            )

            annotation = TableAnnotation(
                database_name=database,
                schema_name=schema,
                table_name=table_name,
                description=description,
                metadata_json=metadata.model_dump_json(),
                embeddings=None,  # to be filled later
                schema_hash=schema_hash,
            )

            return annotation
        except Exception as exc:
            logger.error(
                "Failed to annotate table '%s' (schema '%s') in database '%s': %s",
                table_name,
                schema,
                database,
                exc,
                exc_info=False,
            )
            return None

    async def annotate_database(
        self,
        database: str,
        *,
        skip_if_exists: bool = False,
        max_concurrent: int | None = None,
    ) -> list[TableAnnotation]:
        """Annotate all tables in a given database with controlled concurrency."""
        schemas = tuple(list_schemas_for(database))

        table_targets: list[tuple[str, str]] = []
        with connection_scope(database) as conn:
            for schema in schemas:
                names = list_tables(conn, schema=schema)
                table_targets.extend((schema, name) for name in names)

        logger.info(
            "Annotating %d tables across %d schemas in database '%s'.",
            len(table_targets),
            len(schemas),
            database,
        )

        # Prevent "too many open files"
        if max_concurrent is None:
            max_concurrent = settings.max_concurrent_annotations

        logger.debug(
            "Using max_concurrent_annotations=%d for database '%s'",
            max_concurrent,
            database,
        )
        semaphore = asyncio.Semaphore(max_concurrent)

        async def annotate_with_limit(schema: str, table_name: str):
            async with semaphore:
                return await self.annotate_table(
                    database,
                    schema,
                    table_name,
                    skip_if_exists=skip_if_exists,
                )

        annotations: list[TableAnnotation] = []
        tasks = [
            annotate_with_limit(schema, table_name)
            for schema, table_name in table_targets
        ]

        results = await asyncio.gather(*tasks)
        successful = 0
        skipped = 0
        for result in results:
            if result is not None:
                annotations.append(result)
                successful += 1
            else:
                # Could be skipped or failed (both return None)
                skipped += 1

        if skipped > 0:
            logger.warning(
                ("Database '%s': %d successful, %d skipped/failed out of %d tables."),
                database,
                successful,
                skipped,
                len(table_targets),
            )

        return annotations

    async def save_with_embeddings(
        self, annotations: list[TableAnnotation] | TableAnnotation
    ) -> None:
        """
        Add embeddings to annotations and save to LanceDB.
        Expects the `embeddings` field to be None and fills it with generated embeddings.
        """
        if isinstance(annotations, TableAnnotation):
            annotations = [annotations]

        if not annotations:
            return

        descriptions = [a.description for a in annotations]
        embeddings = await self.embedding_gen.generate_batch(
            descriptions, batch_size=32
        )

        # Attach embeddings
        for annotation, embedding in zip(annotations, embeddings):
            annotation.embeddings = embedding

        await self.annotation_repo.save_batch(annotations)

    async def store_table_descriptions(self) -> None:
        """Store table descriptions for all configured databases in LanceDB."""
        if not settings.annotate_on_startup:
            logger.info(
                "Skipping table annotation store because annotate_on_startup is disabled."
            )
            return

        database_registry = settings.databases
        database_names = (
            database_registry.names() if database_registry is not None else []
        )

        if not database_names:
            logger.info("No databases configured, skipping annotation store.")
            return

        db = get_lance_db()
        open_or_create_table(db, "table_annotations", TableAnnotation)

        save_tasks: list[asyncio.Task[None]] = []
        for database in database_names:
            annotations = await self.annotate_database(database, skip_if_exists=True)
            if not annotations:
                logger.info(
                    "Database '%s' already has up-to-date annotations, skipping save.",
                    database,
                )
                continue

            save_tasks.append(
                asyncio.create_task(self.save_with_embeddings(annotations))
            )

        if save_tasks:
            await asyncio.gather(*save_tasks)
            clear_table_description_cache()
            logger.info("Finished persisting new table annotations.")
        else:
            logger.info("No new table annotations were generated.")

    async def _generate_description(
        self,
        database: str,
        schema: str,
        table_name: str,
        metadata,
        preview: list[dict],
        column_samples: list[str],
    ) -> str:
        """Use the annotation agent to generate a table description."""
        prompt = (
            f"""
        For the table named '{table_name}' in the database '{database}' schema '{schema}',
        generate a concise description based on the following metadata and data preview.
        <metadata>
        {metadata.to_create_table(table_name=f"{schema}.{table_name}")}
        </metadata>
        <preview>
        {preview}
        </preview>
        The following are samples of distinct values from the tables non-numeric columns:
        <distinct_values>
        {column_samples}
        </distinct_values>
        """
        ).strip()

        logger.debug(
            "Prompt for table '%s' (schema '%s') in database '%s': %s",
            table_name,
            schema,
            database,
            prompt,
        )

        result = await table_annotation_agent.run(prompt)

        if not isinstance(result.output, TableDescriptionModel):
            raise RuntimeError("Annotation agent did not return TableDescription.")

        return result.output.description

    async def _get_column_samples(
        self,
        column_repo: ColumnContentRepository,
        table_name: str,
        schema: str,
        limit: int = 10,
    ) -> list[str]:
        """Get column content samples from lanceDB ColumnContent table for a specific table."""
        try:
            results = await column_repo.get_by_table(
                table_name,
                schema=schema,
                columns=["column_name", "content", "num_distinct"],
            )

            return [
                f"Column: {row['column_name']}\n"
                f"Sample values (showing {min(limit, row['num_distinct'])} of {row['num_distinct']}): "
                f"{row['content'][:limit]}"
                for row in results
            ]
        except ValueError:
            logger.warning(
                "Column contents not indexed for %s.%s.%s",
                column_repo.database,
                schema,
                table_name,
            )
            return []


@lru_cache(maxsize=32)
def get_table_descriptions(
    database: str, targets: tuple[tuple[str, str], ...]
) -> list[TableDescription]:
    """
    Retrieve cached descriptions for the requested schema-qualified tables.
    Missing entries yield `description=None`.
    """
    if not targets:
        return []

    db = get_lance_db()
    table_annotations = open_or_create_table(db, "table_annotations", TableAnnotation)

    records = (
        table_annotations.search()
        .where(f"database_name = '{database}'")
        .select(["schema_name", "table_name", "description"])
        .to_list()
    )

    if len(records) == 0:
        logger.warning("No table descriptions found for database: %s", database)

    lookup = {
        (record["schema_name"], record["table_name"]): record["description"]
        for record in records
    }

    return [
        TableDescription(
            schema_name=schema,
            table_name=table,
            description=lookup.get((schema, table)),
        )
        for schema, table in targets
    ]


def clear_table_description_cache() -> None:
    """Clear the cached table descriptions."""
    get_table_descriptions.cache_clear()


# Factory for dependency injection
def get_annotation_service() -> AnnotationService:
    """Create annotation service with dependencies."""
    return AnnotationService(
        annotation_repo=AnnotationRepository(),
        embedding_gen=EmbeddingGenerator(),
    )

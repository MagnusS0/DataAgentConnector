import re
import unicodedata
from typing import Literal

from lancedb.db import Table
from lancedb.query import BooleanQuery, DocumentGranularity, MatchQuery, Occur

from app.models.lance import ColumnContent
from app.repositories.lance_db import (
    batch_insert_async,
    get_lance_db,
    get_lance_db_async,
    open_or_create_table_async,
    sql_literal,
)

# Matches the n-gram index's trigrams; shorter queries have no n-grams to match.
NGRAM_LENGTH = 3


def compact(value: str) -> str:
    """Fold case and accents and strip spaces and punctuation: "T-Shirt" -> "tshirt"."""
    decomposed = unicodedata.normalize("NFKD", value)
    unaccented = "".join(c for c in decomposed if not unicodedata.combining(c))
    return re.sub(r"[\W_]+", "", unaccented.casefold())


class ColumnContentRepository:
    """Data access for ColumnContent in LanceDB."""

    def __init__(self, database: str, *, schema: str | None = None):
        self.database = database
        self.schema = schema
        self.table_name = f"column_contents_{database}"

    def search_fts(
        self,
        query: str,
        *,
        schema: str | None = None,
        top_k: int = 5,
        max_hits: int = 100,
        value_score_ratio: float = 0.5,
    ) -> list[dict]:
        """Full-text search over individual column values, grouped by column.

        Every value in `content` is scored as its own BM25 document. Exact matches
        are boosted over fuzzy (one edit) matches, so typos still find values
        without outranking exact hits. Slots the word search leaves free are
        filled from a trigram index with values that contain the query once
        case, accents, spaces and punctuation are ignored, so "zeppel" finds
        "Led Zeppelin" and "tshirt" finds "T-Shirt". Returns up to `top_k` columns,
        word matches first, each with the `matched_values` scoring at least
        `value_score_ratio` of that column's best value, which drops weak
        matches on short words.
        """
        db = get_lance_db()

        target_schema = schema or self.schema

        try:
            table = db.open_table(self.table_name)
        except Exception:
            raise ValueError(
                f"Column contents for database '{self.database}' have not been indexed. "
                f"Please run indexing first."
            )

        per_value = DocumentGranularity.LIST_ELEMENT
        exact = MatchQuery(query, "content", boost=2.0, document_granularity=per_value)
        # Fuzzy terms bypass the index's lower-casing, so normalise the query first.
        fuzzy = MatchQuery(
            query.lower(),
            "content",
            fuzziness=1,
            prefix_length=1,
            document_granularity=per_value,
        )
        word_hits = self._search_hits(
            table,
            BooleanQuery([(Occur.SHOULD, exact), (Occur.SHOULD, fuzzy)]),
            target_schema,
            max_hits,
        )
        columns = self._group_by_column(word_hits, value_score_ratio, limit=top_k)

        # Fill the remaining slots from trigrams. Shared trigrams alone are mostly
        # noise, so keep only values that contain the whole query once case, accents,
        # spaces and punctuation are ignored. A list-element query targets one
        # field, so this runs as a second search.
        compacted = compact(query)
        substring_columns: dict[int, dict] = {}
        if len(columns) < top_k and len(compacted) >= NGRAM_LENGTH:
            ngram_hits = self._search_hits(
                table,
                MatchQuery(query, "ngram_content", document_granularity=per_value),
                target_schema,
                max_hits,
            )
            # No score cut-off here: the substring check below decides what stays.
            substring_columns = {
                row_id: column
                for row_id, column in self._group_by_column(ngram_hits, 0.0).items()
                if row_id not in columns
            }

        if not columns and not substring_columns:
            return []

        # Fetch each matched column's value list once, rather than once per hit.
        contents = {
            row["_rowid"]: row["content"]
            for row in table.take_row_ids([*columns, *substring_columns])
            .select(["content"])
            .with_row_id()
            .to_list()
        }
        for row_id, column in columns.items():
            values = contents[row_id]
            column["matched_values"] = [values[i] for i in column.pop("value_indexes")]
        for row_id, column in substring_columns.items():
            if len(columns) == top_k:
                break
            values = contents[row_id]
            matched = [
                values[i]
                for i in column.pop("value_indexes")
                if compacted in compact(values[i])
            ]
            if matched:
                columns[row_id] = {**column, "matched_values": matched}

        return list(columns.values())

    @staticmethod
    def _group_by_column(
        hits: list[dict], value_score_ratio: float, *, limit: int | None = None
    ) -> dict[int, dict]:
        """Group per-value hits, best first, into at most `limit` columns."""
        # One row holds one column, so hits sharing a row id share a column.
        columns: dict[int, dict] = {}
        for hit in hits:
            row_id = hit["_rowid"]
            if row_id not in columns:
                if len(columns) == limit:
                    continue
                columns[row_id] = {
                    "schema_name": hit["schema_name"],
                    "table_name": hit["table_name"],
                    "column_name": hit["column_name"],
                    "num_distinct": hit["num_distinct"],
                    "_score": hit["_score"],
                    "value_indexes": [],
                }
            if hit["_score"] >= columns[row_id]["_score"] * value_score_ratio:
                columns[row_id]["value_indexes"].extend(hit["_doc_index"])
        return columns

    def _search_hits(
        self,
        table: Table,
        query: BooleanQuery | MatchQuery,
        schema: str | None,
        max_hits: int,
    ) -> list[dict]:
        """Run one per-value FTS query and return its hits, best first."""
        try:
            search = table.search(query)
            if schema:
                search = search.where(f"schema_name = {sql_literal(schema)}")
            return (
                search.select(
                    [
                        "schema_name",
                        "table_name",
                        "column_name",
                        "num_distinct",
                        "_score",
                    ]
                )
                .with_row_id(True)
                .limit(max_hits)
                .to_list()
            )
        except Exception as e:
            if "no inverted index" in str(e).lower():
                raise ValueError(
                    f"FTS index for database '{self.database}' is incomplete. "
                    f"Please re-run indexing."
                ) from e
            raise

    async def get_by_table(
        self,
        table_name: str,
        *,
        schema: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict]:
        """Get column contents for a specific table."""
        db = await get_lance_db_async()
        try:
            table = await db.open_table(self.table_name)
        except ValueError as exc:
            raise ValueError(
                f"Column contents table '{self.table_name}' does not exist."
            ) from exc

        target_schema = schema or self.schema
        query = table.query().where(f"table_name = '{table_name}'")
        if target_schema:
            query = query.where(f"schema_name = '{target_schema}'")

        if columns:
            query = query.select(columns)

        return await query.to_list()

    async def save_batch(
        self,
        contents: list[ColumnContent],
        *,
        mode: Literal["overwrite", "append"] = "append",
    ) -> None:
        """Save column contents in bulk."""
        if not contents:
            return
        db = await get_lance_db_async()
        if mode == "overwrite":
            # Recreate rather than reopen, so a table written before a field was
            # added takes the model's current schema.
            table = await db.create_table(
                self.table_name, schema=ColumnContent, mode="overwrite"
            )
        else:
            table = await open_or_create_table_async(
                db, self.table_name, schema=ColumnContent
            )
        await batch_insert_async(table, contents)

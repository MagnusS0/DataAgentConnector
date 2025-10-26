from app.db.lance_db import get_lance_db, open_or_create_table
from app.models.lance_schemas import ColumnContent


def search_column_contents(
    database: str,
    query: str,
    top_k: int = 5,
    max_values_shown: int = 20,
) -> list[str]:
    """Search for column contents matching the query in the specified database."""
    db = get_lance_db()
    table_name = f"column_contents_{database}"
    table = open_or_create_table(
        db,
        table_name,
        schema=ColumnContent,
    )

    results = (
        table.search(
            query,
            query_type="fts",
            fts_columns=["content"],
        )
        .select(["table_name", "column_name", "content", "num_distinct", "_score"])
        .limit(top_k)
        .to_list()
    )

    if not results:
        return []

    # Only keep results with score within 70% of the top score
    # if not the result will include noisy weak matches
    if len(results) > 0:
        top_score = results[0]["_score"]
        min_score = top_score * 0.7
        results = [row for row in results if row["_score"] >= min_score]
    if not results:
        return []

    return [
        f"""
        Query '{query}' matched column `{row["column_name"]}` in table `{row["table_name"]}`.
        Showing {min(max_values_shown, row["num_distinct"])} of {row["num_distinct"]} distinct values: {row["content"][:max_values_shown]}
        """.strip()
        for row in results
    ]

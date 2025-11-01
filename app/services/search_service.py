from app.repositories.column_contents import ColumnContentRepository
from app.core.logging import get_logger

logger = get_logger(__name__)


class SearchService:
    """Service for searching column contents."""

    def search_column_contents(
        self,
        database: str,
        query: str,
        top_k: int = 5,
        max_values_shown: int = 20,
        score_threshold: float = 0.7,
    ) -> list[str]:
        """Search for column contents matching query."""
        repo = ColumnContentRepository(database)
        results = repo.search_fts(query, top_k=top_k)

        if not results:
            return []

        # Filter by score threshold
        top_score = results[0]["_score"]
        min_score = top_score * score_threshold
        results = [r for r in results if r["_score"] >= min_score]

        if not results:
            return []

        return [
            f"Query '{query}' matched column `{row['column_name']}` in table `{row['table_name']}`.\n"
            f"Showing {min(max_values_shown, row['num_distinct'])} of {row['num_distinct']} distinct values: "
            f"{row['content'][:max_values_shown]}"
            for row in results
        ]

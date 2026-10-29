import pytest

from app.domain.column_masker import ColumnMasker
from app.repositories.column_contents import ColumnContentRepository
from app.repositories.lance_db import get_lance_db
from app.schemas.config import AnonymizationConfig
from app.services.search_service import SearchService

pytestmark = pytest.mark.usefixtures("indexed")


@pytest.fixture
def repo() -> ColumnContentRepository:
    return ColumnContentRepository("shop")


def test_exact_match_ranks_city_first(repo: ColumnContentRepository) -> None:
    results = repo.search_fts("Oslo")
    top = results[0]
    assert (top["table_name"], top["column_name"]) == ("customers", "city")
    assert "Oslo" in top["matched_values"]


def test_typo_finds_value(repo: ColumnContentRepository) -> None:
    results = repo.search_fts("Metalica")
    values = [v for r in results for v in r["matched_values"]]
    assert "Metallica T-Shirt" in values


def test_accent_insensitive(repo: ColumnContentRepository) -> None:
    results = repo.search_fts("Sao Paulo")
    values = [v for r in results for v in r["matched_values"]]
    assert "São Paulo" in values


def test_results_grouped_by_column(repo: ColumnContentRepository) -> None:
    results = repo.search_fts("Oslo")
    keys = [(r["table_name"], r["column_name"]) for r in results]
    assert len(keys) == len(set(keys))


def test_schema_filter(repo: ColumnContentRepository) -> None:
    assert repo.search_fts("Oslo", schema="main")
    assert repo.search_fts("Oslo", schema="does_not_exist") == []


def test_masked_column_is_not_indexed() -> None:
    table = get_lance_db().open_table("column_contents_shop")
    rows = (
        table.search()
        .where("table_name = 'customers'")
        .select(["column_name"])
        .to_list()
    )
    indexed_columns = {r["column_name"] for r in rows}
    assert "city" in indexed_columns
    assert "ssn" not in indexed_columns


def test_search_drops_columns_the_masker_hides() -> None:
    masker = ColumnMasker(AnonymizationConfig(masked_columns=("city",)))
    assert SearchService().search_column_contents("shop", "Oslo")
    out = SearchService().search_column_contents("shop", "Oslo", masker=masker)
    assert all("`city`" not in line for line in out)


def test_unindexed_database_raises() -> None:
    with pytest.raises(ValueError, match="not been indexed"):
        ColumnContentRepository("ghost").search_fts("x")


def test_service_output_format() -> None:
    out = SearchService().search_column_contents("shop", "Oslo")
    assert out
    assert "Matching values:" in out[0]
    assert "`city`" in out[0]


def test_partial_word_finds_value(repo: ColumnContentRepository) -> None:
    results = repo.search_fts("bicyc")
    values = [v for r in results for v in r["matched_values"]]
    assert "Red Bicycle" in values


def test_punctuation_free_query_finds_value(repo: ColumnContentRepository) -> None:
    results = repo.search_fts("tshirt")
    values = [v for r in results for v in r["matched_values"]]
    assert "Metallica T-Shirt" in values


def test_shared_trigrams_alone_do_not_match(repo: ColumnContentRepository) -> None:
    # "helmsman" shares trigrams with "Blue Helmet" but is not part of it.
    assert repo.search_fts("helmsman") == []

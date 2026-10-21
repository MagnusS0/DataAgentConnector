import pytest

from app.domain.column_masker import get_column_masker
from app.repositories.column_contents import ColumnContentRepository
from app.services.search_service import SearchService


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


def test_masked_column_never_returned() -> None:
    masker = get_column_masker("shop")
    out = SearchService().search_column_contents("shop", "111-11-1111", masker=masker)
    assert all("`ssn`" not in line for line in out)


def test_unindexed_database_raises() -> None:
    with pytest.raises(ValueError, match="not been indexed"):
        ColumnContentRepository("ghost").search_fts("x")


def test_service_output_format() -> None:
    out = SearchService().search_column_contents("shop", "Oslo")
    assert out
    assert "Matching values:" in out[0]
    assert "`city`" in out[0]

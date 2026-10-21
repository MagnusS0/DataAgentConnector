import pytest

from app.domain.fk_analyzer import connect_tables, shortest_join_path
from app.repositories.fk_metadata import get_fk_info


@pytest.fixture
def fk_info():
    return get_fk_info("shop")


def test_path_goes_through_orders(fk_info) -> None:
    steps = shortest_join_path(fk_info, "customers", "products")
    assert len(steps) == 2
    assert "orders" in {steps[0].right_table, steps[1].left_table}
    assert steps[-1].right_table == "products"


def test_direct_fk_is_single_step(fk_info) -> None:
    steps = shortest_join_path(fk_info, "orders", "customers")
    assert len(steps) == 1
    assert ("customer_id", "id") in steps[0].column_pairs


def test_connect_three_tables(fk_info) -> None:
    steps = connect_tables(fk_info, ["customers", "orders", "products"])
    touched = {t for s in steps for t in (s.left_table, s.right_table)}
    assert touched == {"customers", "orders", "products"}


def test_unknown_table_raises(fk_info) -> None:
    with pytest.raises(ValueError):
        shortest_join_path(fk_info, "customers", "nope")
    with pytest.raises(ValueError):
        connect_tables(fk_info, ["customers", "nope"])

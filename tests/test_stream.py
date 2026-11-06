import pytest

from src.events import generate_events, write_json_batches
from src.stream_job import run


def test_generator_has_dups_and_bad_rows():
    ev = generate_events(2000)
    ids = [e["event_id"] for e in ev]
    assert len(ids) > len(set(ids))  # duplicatas
    assert any(e["amount"] < 0 for e in ev)  # inválidos


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    base = tmp_path_factory.mktemp("lake")
    landing = tmp_path_factory.mktemp("landing")
    ev = generate_events(1500)
    write_json_batches(ev, str(landing))
    out = run("files", str(landing), str(base))
    return ev, out, base


def test_bronze_keeps_everything(result):
    ev, out, _ = result
    assert out["bronze"] == len(ev)


def test_silver_dedups_and_filters(result):
    ev, out, _ = result
    expected = len({e["event_id"] for e in ev if e["amount"] > 0})
    assert out["silver"] <= expected
    assert out["silver"] >= expected * 0.9  # watermark pode descartar atrasados extremos
    assert out["silver"] < out["bronze"]


def test_gold_not_empty(result):
    _, out, _ = result
    assert out["gold"] > 0

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date
from pathlib import Path

from cn_market_lake.storage.state import StateStore


def _update_max_worker(meta_root: str, day: int) -> None:
    StateStore(Path(meta_root)).update_max_date("daily_bars", date(2024, 6, day))


def test_state_store_roundtrip(tmp_path):
    store = StateStore(tmp_path / "meta")
    assert store.get_date("daily_bars") is None
    store.set_date("daily_bars", date(2024, 6, 28))
    assert store.get_date("daily_bars") == date(2024, 6, 28)


def test_state_store_update_max(tmp_path):
    store = StateStore(tmp_path / "meta")
    store.update_max_date("daily_bars", date(2024, 6, 1))
    store.update_max_date("daily_bars", date(2024, 6, 28))
    store.update_max_date("daily_bars", date(2024, 6, 15))
    assert store.get_date("daily_bars") == date(2024, 6, 28)


def test_state_store_update_max_concurrent_threads(tmp_path):
    store = StateStore(tmp_path / "meta")
    days = list(range(1, 29))
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda d: store.update_max_date("daily_bars", date(2024, 6, d)), days))
    assert store.get_date("daily_bars") == date(2024, 6, 28)


def test_state_store_update_max_concurrent_processes(tmp_path):
    meta_root = tmp_path / "meta"
    StateStore(meta_root).set_date("daily_bars", date(2024, 6, 1))
    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_update_max_worker, str(meta_root), day) for day in range(2, 29)]
        for fut in futures:
            fut.result()
    store = StateStore(meta_root)
    assert store.get_date("daily_bars") == date(2024, 6, 28)

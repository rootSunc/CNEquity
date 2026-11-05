from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date
from pathlib import Path

from cnequity.storage.state import StateStore


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


def test_state_store_string_set_roundtrip(tmp_path):
    store = StateStore(tmp_path / "meta")
    assert store.get_string_set("adj_factors", "retry_symbols") == set()
    store.set_string_set("adj_factors", "retry_symbols", {"600519.SH", "000001.SZ"})
    assert store.get_string_set("adj_factors", "retry_symbols") == {
        "600519.SH",
        "000001.SZ",
    }
    store.set_string_set("adj_factors", "retry_symbols", [])
    assert store.get_string_set("adj_factors", "retry_symbols") == set()


def test_state_store_retries_transient_windows_replace_denial(tmp_path, monkeypatch):
    """A reader holding the watermark open briefly must not fail the commit."""
    import cnequity.storage.atomic as atomic

    real_replace = atomic.os.replace
    denials = {"left": 2}

    def _flaky_replace(src, dst):
        if denials["left"]:
            denials["left"] -= 1
            raise PermissionError(5, "Access is denied")
        real_replace(src, dst)

    import cnequity.file_replace as file_replace

    monkeypatch.setattr(file_replace, "_REPLACE_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(atomic.os, "replace", _flaky_replace)
    store = StateStore(tmp_path / "meta")
    store.set_date("daily_bars", date(2024, 6, 28))

    assert denials["left"] == 0
    assert store.get_date("daily_bars") == date(2024, 6, 28)
    assert not list((tmp_path / "meta" / "state").glob("*.tmp"))

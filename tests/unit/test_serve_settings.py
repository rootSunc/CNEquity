"""Operations-page lake switches: preview, then write only the whitelist."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnequity.config import load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.serve.app import create_app
from cnequity.serve.ops.catalog import OPS_BY_ID
from cnequity.serve.ops.settings import (
    SETTINGS,
    OpsError,
    normalize,
    read_values,
    revise,
)

SECRET = "super-secret-token"
API_KEY = "hithink-secret-key"
PROXY = "http://user:secret-proxy@127.0.0.1:9"


def _client(config, **kwargs):
    return TestClient(create_app(config, **kwargs), base_url="http://127.0.0.1")


def _headers(client) -> dict:
    home = client.get("/api/ops")
    assert home.status_code == 200, home.text
    return {"Origin": "http://127.0.0.1", "X-CNE-CSRF": home.json()["csrf_token"]}


def _values(payload: dict, **overrides):
    values = {
        item["id"]: item["value"] for section in payload["sections"] for item in section["settings"]
    }
    values.update(overrides)
    return values


def _rich(tmp_path: Path) -> Path:
    path = tmp_path / "cnequity.toml"
    path.write_bytes(
        f"""# keep this comment
[data]
root = "{path_for_toml(tmp_path / "data")}"

[sources.eastmoney]
enabled = true # primary
push2_paused = false
proxy = "{PROXY}"
min_interval_seconds = 0.5

[sources.tushare]
enabled = false
token = "{SECRET}"

[sources.ths_official]
enabled = false
api_key = "{API_KEY}"

[minute_bars]
enabled = false
scope = "index:000300.SH"
symbols = ["600519.SH"]
""".encode()
    )
    return path


def test_the_whitelist_is_the_page_and_daily_has_no_range():
    assert [spec.id for spec in SETTINGS] == [
        "push2_paused",
        "tdx",
        "eastmoney",
        "sina_bars",
        "baostock",
        "bse",
        "ths",
        "cninfo",
        "tushare",
        "ths_official",
        "minute_bars",
        "trade_ticks",
        "futures",
        "futures_minute",
        "ingest",
        "ingest_listed_funds",
        "ingest_eligible_etfs",
    ]
    for op_id in ("daily.full", "daily.group", "daily.stale"):
        names = {param.name for param in OPS_BY_ID[op_id].params}
        assert "start" not in names
        assert "end" not in names
        assert "daily_bars" not in names


def test_a_config_without_paid_sources_reads_them_off():
    values = read_values('[data]\nroot = "/tmp/lake"\n')
    assert values["tushare"] is False
    assert values["ths_official"] is False


def test_paid_sources_default_off_and_a_toggle_keeps_the_token(tmp_path):
    path = _rich(tmp_path)
    config = load_config(path)
    client = _client(config)
    headers = _headers(client)
    body = client.get("/api/ops/settings").json()
    licensed = next(section for section in body["sections"] if section["id"] == "licensed")
    assert [(item["id"], item["value"]) for item in licensed["settings"]] == [
        ("tushare", False),
        ("ths_official", False),
    ]
    assert "付费数据源" in licensed["title"]

    preview = client.post(
        "/api/ops/settings/preview",
        headers=headers,
        json={"values": _values(body, tushare=True)},
    )
    assert preview.status_code == 200, preview.text
    assert {item["id"] for item in preview.json()["changes"]} == {"tushare"}
    assert SECRET not in preview.text

    applied = client.post(
        "/api/ops/settings/apply",
        headers=headers,
        json={"token": preview.json()["token"], "acknowledged": True},
    )
    assert applied.status_code == 200, applied.text
    written = path.read_text(encoding="utf-8")
    assert f'token = "{SECRET}"' in written
    assert f'api_key = "{API_KEY}"' in written
    tushare = written.split("[sources.tushare]", 1)[1].split("[", 1)[0]
    official = written.split("[sources.ths_official]", 1)[1].split("[", 1)[0]
    assert "enabled = true" in tushare
    assert "enabled = false" in official
    assert client.app.state.config.sources["tushare"] is True
    assert client.app.state.config.sources["ths_official"] is False


def test_preview_does_not_write_and_apply_keeps_secrets(tmp_path):
    path = _rich(tmp_path)
    original = path.read_bytes()
    config = load_config(path)
    client = _client(config)
    headers = _headers(client)
    home = client.get("/api/ops/settings")
    assert home.status_code == 200, home.text
    body = home.json()
    assert SECRET not in home.text
    assert API_KEY not in home.text
    assert "secret-proxy" not in home.text
    assert body["push2_env_note"] is None
    minute = next(
        item
        for section in body["sections"]
        if section["id"] == "capture"
        for item in section["settings"]
        if item["id"] == "minute_bars"
    )
    assert "600519.SH" in minute["scope"]
    assert "范围仍在配置文件里" in minute["scope"]

    same = client.post("/api/ops/settings/preview", headers=headers, json={"values": _values(body)})
    assert same.status_code == 200, same.text
    assert same.json()["token"] is None
    assert path.read_bytes() == original

    preview = client.post(
        "/api/ops/settings/preview",
        headers=headers,
        json={"values": _values(body, push2_paused=True, minute_bars=True, ingest="all_a_sh_sz")},
    )
    assert preview.status_code == 200, preview.text
    shown = preview.json()
    assert shown["token"]
    assert "定时日更、收尾补抓和之后的命令都会按新值执行" in shown["acknowledgement"]
    assert {item["id"] for item in shown["changes"]} == {"push2_paused", "minute_bars", "ingest"}
    assert SECRET not in preview.text
    assert API_KEY not in preview.text
    assert path.read_bytes() == original
    assert client.get("/api/ops/jobs").json() == []

    refused = client.post(
        "/api/ops/settings/apply",
        headers=headers,
        json={"token": shown["token"], "acknowledged": False},
    )
    assert refused.status_code == 422
    assert path.read_bytes() == original

    applied = client.post(
        "/api/ops/settings/apply",
        headers=headers,
        json={"token": shown["token"], "acknowledged": True},
    )
    assert applied.status_code == 200, applied.text
    saved = applied.json()
    assert saved["saved"] is True
    assert SECRET not in applied.text
    backups = list(path.parent.glob(path.name + ".bak-*"))
    assert len(backups) == 1
    assert backups[0].name == saved["backup_name"]
    assert backups[0].read_bytes() == original
    written = path.read_bytes().decode()
    assert "# keep this comment" in written
    assert "enabled = true # primary" in written
    assert "push2_paused = true" in written
    assert f'token = "{SECRET}"' in written
    assert f'api_key = "{API_KEY}"' in written
    assert PROXY in written
    assert "min_interval_seconds = 0.5" in written
    assert 'scope = "index:000300.SH"' in written
    loaded = tomllib_text(written)
    assert loaded["sources"]["eastmoney"]["push2_paused"] is True
    assert loaded["minute_bars"]["enabled"] is True
    assert loaded["universe"]["ingest"] == "all_a_sh_sz"
    assert client.app.state.config.eastmoney_push2_paused is True
    assert client.app.state.config.minute_bars_enabled is True
    assert client.app.state.config.ingest_universe == "all_a_sh_sz"
    assert client.app.state.view.config is client.app.state.config
    again = client.post(
        "/api/ops/settings/apply",
        headers=headers,
        json={"token": shown["token"], "acknowledged": True},
    )
    assert again.status_code == 422
    assert client.get("/api/ops/jobs").json() == []


def test_env_pause_is_reported_without_rewriting_the_file(config, monkeypatch):
    monkeypatch.setenv("CNE_PUSH2_PAUSED", "1")
    client = _client(config)
    home = client.get("/api/ops/settings")
    assert home.status_code == 200, home.text
    body = home.json()
    assert body["push2_env_paused"] is True
    assert "关掉配置" in body["push2_env_note"]
    push2 = next(
        item
        for section in body["sections"]
        for item in section["settings"]
        if item["id"] == "push2_paused"
    )
    assert push2["value"] is False


def test_a_changed_file_and_unknown_values_are_refused(config):
    client = _client(config)
    headers = _headers(client)
    home = client.get("/api/ops/settings").json()
    path = Path(config.config_path)
    original = path.read_bytes()
    preview = client.post(
        "/api/ops/settings/preview",
        headers=headers,
        json={"values": _values(home, tdx=False)},
    )
    assert preview.status_code == 200, preview.text
    path.write_bytes(original + b"\n# edited outside\n")
    applied = client.post(
        "/api/ops/settings/apply",
        headers=headers,
        json={"token": preview.json()["token"], "acknowledged": True},
    )
    assert applied.status_code == 422
    assert b"edited outside" in path.read_bytes()
    assert b"enabled = false" not in path.read_bytes().split(b"[tdx_protocol]")[-1].split(b"[")[0]

    unknown = client.post(
        "/api/ops/settings/preview",
        headers=headers,
        json={"values": {**_values(home), "proxy": "http://127.0.0.1:9"}},
    )
    assert unknown.status_code == 422
    typed = client.post(
        "/api/ops/settings/preview",
        headers=headers,
        json={"values": {**_values(home), "push2_paused": "true"}},
    )
    assert typed.status_code == 422
    assert path.read_bytes().endswith(b"# edited outside\n")


@pytest.mark.usefixtures("chinese_output")
def test_inline_and_shorthand_tables_are_not_rewritten(tmp_path):
    path = tmp_path / "inline.toml"
    text = f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[sources]
eastmoney = {{ enabled = true, push2_paused = false, token = "{SECRET}" }}
"""
    path.write_text(text, encoding="utf-8")
    current = read_values(text)
    assert current["push2_paused"] is False
    assert current["eastmoney"] is True
    with pytest.raises(OpsError, match="标准 TOML"):
        revise(text, {**current, "push2_paused": True})
    shorthand = text.replace(
        'eastmoney = { enabled = true, push2_paused = false, token = "' + SECRET + '" }',
        "eastmoney = false",
    )
    assert read_values(shorthand)["eastmoney"] is False
    with pytest.raises(OpsError, match="标准 TOML"):
        revise(shorthand, {**read_values(shorthand), "eastmoney": True})
    assert SECRET not in str(read_values(text))


def test_comments_and_crlf_survive_a_single_key_edit():
    text = "# head\r\n[tdx_protocol]\r\nallow_mock = true # demo\r\n"
    current = read_values(text)
    edited, changes = revise(text, {**current, "tdx": False})
    assert changes[0]["id"] == "tdx"
    assert "# head\r\n" in edited
    assert "allow_mock = true # demo\r\n" in edited
    assert "enabled = false\r\n" in edited
    assert "\n[tdx_protocol]\n" not in edited


def test_read_only_and_remote_follow_the_operations_page(config):
    readonly = _client(config, read_only=True)
    assert readonly.get("/api/ops/settings").status_code == 200
    assert readonly.post("/api/ops/settings/preview", json={"values": {}}).status_code == 404

    remote = TestClient(create_app(config, token="s3cret"), base_url="http://example.test")
    headers = {"Authorization": "Bearer s3cret"}
    assert remote.get("/api/ops/settings", headers=headers).status_code == 200
    page = remote.get("/api/ops", headers=headers)
    denied = remote.post(
        "/api/ops/settings/preview",
        headers={
            **headers,
            "Origin": "http://example.test",
            "X-CNE-CSRF": page.json()["csrf_token"],
        },
        json={"values": {}},
    )
    assert denied.status_code == 403

    allowed = TestClient(
        create_app(config, token="s3cret", allow_remote_ops=True), base_url="http://example.test"
    )
    page = allowed.get("/api/ops", headers=headers)
    home = allowed.get("/api/ops/settings", headers=headers)
    assert home.status_code == 200
    preview = allowed.post(
        "/api/ops/settings/preview",
        headers={
            **headers,
            "Origin": "http://example.test",
            "X-CNE-CSRF": page.json()["csrf_token"],
        },
        json={"values": _values(home.json())},
    )
    assert preview.status_code == 200
    assert preview.json()["token"] is None


@pytest.mark.usefixtures("chinese_output")
def test_normalize_rejects_a_partial_body():
    with pytest.raises(OpsError, match="不完整"):
        normalize({"push2_paused": True})


def tomllib_text(text: str) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        import tomli as tomllib

    return tomllib.loads(text)

from __future__ import annotations

from pathlib import Path

from quant.bootstrap.config_loader import ConfigLoader



def _write_yaml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")



def test_config_loader_loads_expected_sections(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "app.yaml", "app:\n  timezone: Asia/Seoul\n")
    _write_yaml(
        tmp_path / "providers.yaml",
        "krx:\n"
        "  enabled: true\n"
        "  mode: placeholder\n"
        "  base_url: null\n"
        "  timeout_sec: 15\n"
        "  endpoints:\n"
        "    instrument_master: /v1/instruments\n"
        "  TODO: test-note\n",
    )
    _write_yaml(tmp_path / "storage.yaml", "parquet:\n  raw_root: data/raw\n")
    _write_yaml(tmp_path / "universe" / "core.yaml", "min_turnover: 100\n")
    _write_yaml(tmp_path / "feature_sets" / "v1.yaml", "benchmark_family: KOSPI\n")

    loader = ConfigLoader(config_root=tmp_path)

    assert loader.load_app_config()["app"]["timezone"] == "Asia/Seoul"
    assert loader.load_provider_config()["krx"]["enabled"] is True
    runtime = loader.get_provider_runtime_config("krx")
    assert runtime == {
        "enabled": True,
        "mode": "placeholder",
        "note": "test-note",
        "base_url": None,
        "timeout_sec": 15,
        "endpoints": {"instrument_master": "/v1/instruments"},
    }
    assert loader.load_storage_config()["parquet"]["raw_root"] == "data/raw"
    assert loader.load_universe_config("core")["min_turnover"] == 100
    assert loader.load_feature_set_config("v1")["benchmark_family"] == "KOSPI"



def test_config_loader_blocks_path_traversal(tmp_path: Path) -> None:
    loader = ConfigLoader(config_root=tmp_path)
    try:
        loader.load_yaml("../outside.yaml")
    except ValueError:
        return
    raise AssertionError("Expected ValueError for path traversal attempt")

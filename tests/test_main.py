import pytest
from fastapi.testclient import TestClient
from main import app, build_media_request_params, validate_config, _new_runtime_state, get_current_config

client = TestClient(app)

def test_health():
    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert "state" in data
    assert "config" in data

def test_get_config():
    response = client.get("/api/config")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert "source_url" in data["config"]

def test_validate_config_valid():
    raw_cfg = {
        "download_root": "./test_downloads",
        "download_limit": 5,
        "duration_minutes": 20,
        "download_thumbnails": False,
    }
    validated = validate_config(raw_cfg)
    assert validated["download_root"] == "./test_downloads"
    assert validated["download_limit"] == 5
    assert validated["video_format"] == "original"
    assert validated["download_thumbnails"] is False
    assert validated["min_time"] == 20 * 60
    assert validated["max_time"] == 20 * 60

def test_validate_config_invalid_limit():
    with pytest.raises(ValueError, match="下载数量必须在 1 到 100 之间"):
        validate_config({"download_root": "dl", "download_limit": 200})

def test_validate_config_force_original_format():
    validated = validate_config({"download_root": "dl", "video_format": "avi"})
    assert validated["video_format"] == "original"

def test_validate_config_invalid_time():
    with pytest.raises(ValueError, match="最小时长不能大于最大时长"):
        validate_config({"download_root": "dl", "min_time": 100, "max_time": 50})

def test_build_media_request_params_include_time_filters():
    params = build_media_request_params(
        validate_config(
            {
                "download_root": "dl",
                "download_limit": 5,
                "min_time": 1200,
                "max_time": 1800,
            }
        )
    )
    assert params["min_time"] == 1200
    assert params["max_time"] == 1800
    assert "minTime" not in params
    assert "maxTime" not in params

def test_save_config():
    payload = {
        "download_root": "./test_save",
        "download_limit": 3
    }
    response = client.post("/api/config", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["config"]["download_limit"] == 3

def test_download_status_endpoint():
    response = client.get("/api/download/status")
    assert response.status_code == 200
    data = response.json()
    assert "state" in data
    assert "logs" in data
    assert data["state"]["is_running"] is False

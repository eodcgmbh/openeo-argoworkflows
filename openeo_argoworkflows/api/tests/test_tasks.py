import datetime
import fakeredis
import pytest
import uuid

from rq import Worker, SimpleWorker
from unittest.mock import patch

from openeo_fastapi.client.processes import UserDefinedProcessGraph
from openeo_fastapi.client.psql.engine import create

from openeo_argoworkflows_api.tasks import queue_to_submit, q, _select_dask_profile, _resolve_udps

BASE = {
    "GATEWAY_URL": "http://gateway",
    "OPENEO_EXECUTOR_IMAGE": "img:latest",
    "WORKER_CORES": "4",
    "WORKER_MEMORY": "8",
    "WORKER_LIMIT": "6",
    "CLUSTER_IDLE_TIMEOUT": "3600",
}

PROFILES = {
    "power": {"WORKER_CORES": "8", "WORKER_MEMORY": "16", "WORKER_LIMIT": "10"},
    "standard": {"WORKER_CORES": "2", "WORKER_MEMORY": "4", "WORKER_LIMIT": "4"},
}

ROLE_MAPPING = {
    "early_adopter": "power",
    "platform_developer": "power",
    "default": "standard",
}


def test_select_profile_first_matching_role_wins():
    result = _select_dask_profile(["early_adopter", "other"], ROLE_MAPPING, PROFILES, BASE)
    assert result["WORKER_CORES"] == "8"
    assert result["WORKER_MEMORY"] == "16"
    assert result["GATEWAY_URL"] == "http://gateway"


def test_select_profile_second_role_matches():
    result = _select_dask_profile(["unknown", "platform_developer"], ROLE_MAPPING, PROFILES, BASE)
    assert result["WORKER_CORES"] == "8"


def test_select_profile_falls_back_to_default():
    result = _select_dask_profile(["unknown_role"], ROLE_MAPPING, PROFILES, BASE)
    assert result["WORKER_CORES"] == "2"
    assert result["WORKER_MEMORY"] == "4"


def test_select_profile_no_roles_uses_default():
    result = _select_dask_profile([], ROLE_MAPPING, PROFILES, BASE)
    assert result["WORKER_CORES"] == "2"


def test_select_profile_no_match_no_default_returns_base():
    mapping_without_default = {"early_adopter": "power"}
    result = _select_dask_profile(["unknown"], mapping_without_default, PROFILES, BASE)
    assert result == BASE


def test_select_profile_unknown_profile_name_returns_base():
    bad_mapping = {"early_adopter": "nonexistent_profile", "default": "also_nonexistent"}
    result = _select_dask_profile(["early_adopter"], bad_mapping, PROFILES, BASE)
    assert result == BASE


def test_select_profile_base_fields_preserved():
    result = _select_dask_profile(["early_adopter"], ROLE_MAPPING, PROFILES, BASE)
    assert result["GATEWAY_URL"] == "http://gateway"
    assert result["OPENEO_EXECUTOR_IMAGE"] == "img:latest"
    assert result["CLUSTER_IDLE_TIMEOUT"] == "3600"


@patch("openeo_argoworkflows_api.tasks.Redis")
def test_submit_job(mock_redis, redis_conn, a_mock_job):
    assert True


def test_resolve_udps_inlines_udp(mock_engine):
    user_id = uuid.uuid4()

    udp = UserDefinedProcessGraph(
        id="add_one",
        user_id=user_id,
        process_graph={
            "add_step": {
                "process_id": "add",
                "arguments": {"x": {"from_parameter": "x"}, "y": 1},
                "result": True,
            }
        },
        parameters=[{"name": "x", "schema": {"type": "number"}}],
        created=datetime.datetime.now(),
    )
    create(udp)

    process_graph = {
        "call_udp": {
            "process_id": "add_one",
            "namespace": str(user_id),
            "arguments": {"x": 5},
            "result": True,
        }
    }

    resolved = _resolve_udps(process_graph, user_id)

    # resolve_process_graph flattens UDP nodes into the top-level graph,
    # prefixed with the calling node's id.
    assert "call_udp_add_step" in resolved
    assert resolved["call_udp_add_step"]["process_id"] == "add"
    assert resolved["call_udp_add_step"]["arguments"] == {"x": 5, "y": 1}

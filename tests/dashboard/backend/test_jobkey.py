"""Tests for deriving logical job keys from selected assets."""

import httpx
import pytest

from sds_utils.dashboard.backend import jobkey


def test_dependency_yaml_output_names_match_dagster_asset_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    yaml_text = """
(l2, summed-intensity):
  partition: repoint
  inputs:
    - source: hit
      data_type: l0
      descriptor: raw
  outputs:
    - source: hit
      data_type: l2
      descriptor: summed-intensity
"""

    def fake_get(url: str, *, timeout: int) -> httpx.Response:
        assert url.endswith("/imap_hit_dependencies.yaml")
        return httpx.Response(200, text=yaml_text, request=httpx.Request("GET", url))

    monkeypatch.setattr(jobkey.httpx, "get", fake_get)
    jobkey._dependencies_for_instrument.cache_clear()
    jobkey._job_outputs_for_instrument.cache_clear()
    try:
        parts = jobkey.derive_job_key("__ASSET_JOB", [["hit_l2_summedintensity"]])
        definitions = jobkey.current_job_definitions(["hit"])
    finally:
        jobkey._dependencies_for_instrument.cache_clear()
        jobkey._job_outputs_for_instrument.cache_clear()

    assert parts == ("hit_l2_summedintensity", "hit", "l2", "summedintensity")
    assert definitions["hit_l2_summedintensity"] == jobkey.CurrentJobDefinition(
        job_key="hit_l2_summedintensity",
        instrument="hit",
        data_level="l2",
        descriptor="summedintensity",
        partition_type="repoint",
        expected_assets=frozenset({("hit_l2_summedintensity",)}),
    )
    assert definitions["hit_l0_none"] == jobkey.CurrentJobDefinition(
        job_key="hit_l0_none",
        instrument="hit",
        data_level="l0",
        descriptor="none",
        partition_type="repoint",
        expected_assets=frozenset({("hit_l0_raw",)}),
    )


def test_exact_yaml_output_set_resolves_named_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        jobkey,
        "_job_outputs_for_instrument",
        lambda instrument: {
            frozenset({"glows_l1a_de", "glows_l1a_hist"}): ("l1a", "all")
        },
    )

    parts = jobkey.derive_job_key(
        "__ASSET_JOB",
        [["glows_l1a_hist"], ["glows_l1a_de"]],
    )

    assert parts == ("glows_l1a_all", "glows", "l1a", "all")


def test_unmatched_assets_fall_back_to_lowest_non_ancillary_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(jobkey, "_job_outputs_for_instrument", lambda instrument: {})

    parts = jobkey.derive_job_key(
        "__ASSET_JOB",
        [
            ["glows_ancillary_speed"],
            ["glows_l3e_survivalprobability"],
            ["glows_l1a_de"],
        ],
    )

    assert parts == ("glows_l1a", "glows", "l1a", None)


def test_partial_output_set_does_not_claim_yaml_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        jobkey,
        "_job_outputs_for_instrument",
        lambda instrument: {
            frozenset({"glows_l1a_de", "glows_l1a_hist"}): ("l1a", "all")
        },
    )

    parts = jobkey.derive_job_key("__ASSET_JOB", [["glows_l1a_de"]])

    assert parts == ("glows_l1a", "glows", "l1a", None)


def test_named_job_does_not_consult_selected_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        jobkey,
        "_job_outputs_for_instrument",
        lambda instrument: pytest.fail("YAML should not be loaded for named jobs"),
    )

    parts = jobkey.derive_job_key(
        "hit_l2_summedintensity_processing_job", [["other_l0_raw"]]
    )

    assert parts == ("hit_l2_summedintensity", "hit", "l2", "summedintensity")


def test_mixed_instruments_cannot_define_one_job_key() -> None:
    parts = jobkey.derive_job_key(
        "__ASSET_JOB", [["hit_l2_summedintensity"], ["glows_l1a_de"]]
    )

    assert parts == (None, None, None, None)

"""Tests for ingesting job-independent L0 materializations."""

import asyncio
import datetime
from typing import cast

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from sds_utils.dashboard.backend.db.models import (
    CacheIngestionState,
    CachedAssetMaterialization,
    DagsterCacheNamespace,
)
from sds_utils.dashboard.backend.query.graphql_api import DagsterGraphQLClient
from sds_utils.dashboard.backend.query.graphql_api.l_0_materializations import (
    L0Materializations,
)
from sds_utils.dashboard.backend.query.l0_ingestion import (
    L0_INGESTION_STREAM,
    ingest_l0_materializations,
)


class FakeClient:
    """Return predefined asset-materialization responses."""

    def __init__(self, responses: list[L0Materializations]) -> None:
        self.responses = iter(responses)
        self.asset_keys: list[list[str]] = []

    async def l_0_materializations(
        self,
        *,
        asset_key: object,
        limit: int,
        before_timestamp_millis: str,
    ) -> L0Materializations:
        self.asset_keys.append(asset_key.path)  # type: ignore[attr-defined]
        return next(self.responses)


def _response() -> L0Materializations:
    timestamp = datetime.datetime(2026, 9, 12, 12, tzinfo=datetime.UTC)
    return L0Materializations.model_validate(
        {
            "assetNodeOrError": {
                "__typename": "AssetNode",
                "assetMaterializations": [
                    {
                        "runId": "sensor-run",
                        "timestamp": str(timestamp.timestamp() * 1000),
                        "partition": (
                            "repoint369_2026-09-12T10:03:12_to_"
                            "2026-09-13T10:03:10"
                        ),
                        "assetKey": {"path": ["glows_l0_raw"]},
                        "metadataEntries": [
                            {
                                "__typename": "TextMetadataEntry",
                                "label": "source",
                                "text": "imap",
                            }
                        ],
                    }
                ],
            }
        }
    )


def test_ingest_l0_materializations_stores_events_and_watermarks(
    monkeypatch,
) -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    with Session(db_engine) as session:
        session.add(
            DagsterCacheNamespace(
                name="default",
                graphql_url="https://dagster.example/graphql",
            )
        )
        session.commit()
    monkeypatch.setattr(
        "sds_utils.dashboard.backend.query.l0_ingestion._l0_asset_keys",
        lambda _instruments: ["glows_l0_raw"],
    )
    client = FakeClient([_response()])
    start = datetime.datetime(2026, 9, 1, tzinfo=datetime.UTC)
    end = datetime.datetime(2026, 9, 20, tzinfo=datetime.UTC)

    count = asyncio.run(
        ingest_l0_materializations(
            start,
            end,
            instruments=["glows"],
            db_engine=db_engine,
            client=cast(DagsterGraphQLClient, client),
        )
    )

    assert count == 1
    assert client.asset_keys == [["glows_l0_raw"]]
    with Session(db_engine) as session:
        event = session.exec(select(CachedAssetMaterialization)).one()
        state = session.exec(select(CacheIngestionState)).one()
    assert event.asset_key == "glows_l0_raw"
    assert event.partition_label == "repoint"
    assert event.repoint == 369
    assert event.event_metadata == {"source": "imap"}
    assert state.stream == L0_INGESTION_STREAM
    assert state.watermark_start == start.replace(tzinfo=None)
    assert state.watermark_end == end.replace(tzinfo=None)

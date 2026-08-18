#!/usr/bin/env python

import datetime
import unittest.mock

import pytest

import scrubber


class TestScrubber:
    @pytest.mark.parametrize(
        "today, expected",
        [
            (
                "20251215",
                [(2025, 9), (2025, 10), (2025, 11), (2025, 12)],
            ),
            (
                "20270203",
                [(2025, 9), (2025, 10), (2025, 11), (2025, 12)]
                + [(2026, i) for i in range(1, 13)]
                + [(2027, 1), (2027, 2)],
            ),
        ],
    )
    def test_yearmonth_iterator(self, today, expected):
        # partial-mocking datetime requires doing a from import in the scrubber, which then
        # obscures access to the rest of the datetime module namespace
        class FakeDT(datetime.datetime):
            def now(*arg, **kwargs):
                return datetime.datetime.strptime(f"{today}T12:01", "%Y%m%dT%H:%M")

        olddt = datetime.datetime
        try:
            datetime.datetime = FakeDT
            output = scrubber.yearmonth_iterator()
        finally:
            datetime.datetime = olddt
        assert output == expected

    @unittest.mock.patch("scrubber.imap_data_access.query")
    @unittest.mock.patch.object(
        scrubber.imap_data_access, "VALID_INSTRUMENTS", ("codice", "glows")
    )
    def test_all_latest_files(self, mock_query):
        mock_query.return_value = []

        class FakeDT(datetime.datetime):
            def now(*arg, **kwargs):
                return datetime.datetime(2026, 2, 1, 12)

        olddt = datetime.datetime
        try:
            datetime.datetime = FakeDT
            latest = scrubber.all_latest_files()
        finally:
            datetime.datetime = olddt
        expected = [
            unittest.mock.call(
                instrument=inst,
                start_date=start_date,
                end_date=end_date,
                version="latest",
            )
            for inst in ("codice", "glows")
            for start_date, end_date in zip(
                (
                    "20250901",
                    "20251001",
                    "20251101",
                    "20251201",
                    "20260101",
                    "20260201",
                ),
                (
                    "20250930",
                    "20251031",
                    "20251130",
                    "20251231",
                    "20260131",
                    "20260228",
                ),
            )
        ]
        assert mock_query.call_args_list == expected
        assert latest == {"codice": [], "glows": []}

    def test_break_by_logical_source(self):
        inputs = [
            {
                "file_path": "imap/swe/l2/2025/10/imap_swe_l2_sci_20251021_v001.0025.cdf",
                "instrument": "swe",
                "data_level": "l2",
                "descriptor": "sci",
                "start_date": "20251021",
            },
            {
                "file_path": "imap/swapi/l3a/2025/11/imap_swapi_l3a_proton-sw_20251111_v001.0012.cdf",
                "instrument": "swapi",
                "data_level": "l3a",
                "descriptor": "proton-sw",
                "start_date": "20251111",
            },
            {
                "file_path": "imap/swe/l2/2025/10/imap_swe_l2_sci_20251022_v001.0026.cdf",
                "instrument": "swe",
                "data_level": "l2",
                "descriptor": "sci",
                "start_date": "20251022",
            },
            {
                "file_path": "imap/swapi/l3a/2025/11/imap_swapi_l3a_proton-sw_20251109_v001.0012.cdf",
                "instrument": "swapi",
                "data_level": "l3a",
                "descriptor": "proton-sw",
                "start_date": "20251109",
            },
            {
                "file_path": "imap/swe/l2/2025/10/imap_swe_l2_sci_20251023_v001.0022.cdf",
                "instrument": "swe",
                "data_level": "l2",
                "descriptor": "sci",
                "start_date": "20251023",
            },
        ]
        expected = {
            ("swe", "l2", "sci"): [
                {
                    "file_path": "imap/swe/l2/2025/10/imap_swe_l2_sci_20251021_v001.0025.cdf",
                    "instrument": "swe",
                    "data_level": "l2",
                    "descriptor": "sci",
                    "start_date": "20251021",
                },
                {
                    "file_path": "imap/swe/l2/2025/10/imap_swe_l2_sci_20251022_v001.0026.cdf",
                    "instrument": "swe",
                    "data_level": "l2",
                    "descriptor": "sci",
                    "start_date": "20251022",
                },
                {
                    "file_path": "imap/swe/l2/2025/10/imap_swe_l2_sci_20251023_v001.0022.cdf",
                    "instrument": "swe",
                    "data_level": "l2",
                    "descriptor": "sci",
                    "start_date": "20251023",
                },
            ],
            ("swapi", "l3a", "proton-sw"): [
                {
                    "file_path": "imap/swapi/l3a/2025/11/imap_swapi_l3a_proton-sw_20251109_v001.0012.cdf",
                    "instrument": "swapi",
                    "data_level": "l3a",
                    "descriptor": "proton-sw",
                    "start_date": "20251109",
                },
                {
                    "file_path": "imap/swapi/l3a/2025/11/imap_swapi_l3a_proton-sw_20251111_v001.0012.cdf",
                    "instrument": "swapi",
                    "data_level": "l3a",
                    "descriptor": "proton-sw",
                    "start_date": "20251111",
                },
            ],
        }
        output = scrubber.break_by_logical_source(inputs)
        assert output == expected

    def test_missing_repoints(self):
        inputs = [
            {"repointing": 81},
            {"repointing": 82},
            {"repointing": 83},
            {"repointing": 84},
            {"repointing": 85},
            {"repointing": 86},
            {"repointing": 87},
            {"repointing": 88},
            {"repointing": 124},
            {"repointing": 125},
            {"repointing": 126},
            {"repointing": 127},
            {"repointing": 128},
            {"repointing": 129},
            {"repointing": 130},
            {"repointing": 131},
            {"repointing": 133},
            {"repointing": 134},
            {"repointing": 135},
            {"repointing": 136},
            {"repointing": 137},
            {"repointing": 138},
            {"repointing": 139},
            {"repointing": 140},
            {"repointing": 141},
            {"repointing": 142},
            {"repointing": 143},
            {"repointing": 144},
            {"repointing": 147},
            {"repointing": 148},
        ]
        expected = list(range(89, 124)) + [132, 145, 146]
        output = scrubber.missing_repoints(inputs)
        assert output == expected

    def test_combine_missing(self):
        inputs = {
            ("glows", "l2", "hist"): [1, 2, 9],
            ("glows", "l3a", "hist"): [2, 3, 9],
            ("hi", "l1a", "45sensor-de"): [1, 6, 7, 8, 9],
            ("hi", "l1a", "90sensor-de"): [4, 5, 6, 9],
            ("hi", "l1b", "45-sensor-de"): [9, 10],
        }
        expected = {
            ("glows", "l2", "all"): [1],
            ("glows", "l3a", "all"): [3],
            ("glows", "all", "all"): [2],
            ("hi", "l1a", "45sensor-de"): [1, 7, 8],
            ("hi", "l1a", "90sensor-de"): [4, 5],
            ("hi", "l1a", "all"): [6],
            ("hi", "l1b", "all"): [10],
            ("all", "all", "all"): [9],
        }
        output = scrubber.combine_missing(inputs)
        assert output == expected

    def test_missing_days(self):
        inputs = [
            {"start_date": "20251120"},
            {"start_date": "20251121"},
            {"start_date": "20251122"},
            {"start_date": "20251123"},
            {"start_date": "20251124"},
            {"start_date": "20251125"},
            {"start_date": "20251126"},
            {"start_date": "20251127"},
            {"start_date": "20251128"},
            {"start_date": "20251202"},
            {"start_date": "20251203"},
            {"start_date": "20251204"},
            {"start_date": "20251205"},
            {"start_date": "20251206"},
            {"start_date": "20251207"},
            {"start_date": "20251208"},
            {"start_date": "20251209"},
            {"start_date": "20251210"},
            {"start_date": "20251211"},
            {"start_date": "20251212"},
            {"start_date": "20251213"},
            {"start_date": "20251214"},
            {"start_date": "20251215"},
            {"start_date": "20251216"},
            {"start_date": "20251217"},
            {"start_date": "20251218"},
            {"start_date": "20251220"},
            {"start_date": "20251221"},
            {"start_date": "20251222"},
            {"start_date": "20251223"},
            {"start_date": "20251224"},
            {"start_date": "20251225"},
            {"start_date": "20251226"},
            {"start_date": "20251227"},
            {"start_date": "20251228"},
            {"start_date": "20251229"},
            {"start_date": "20251230"},
            {"start_date": "20251231"},
            {"start_date": "20260101"},
            {"start_date": "20260102"},
            {"start_date": "20260103"},
            {"start_date": "20260104"},
            {"start_date": "20260112"},
            {"start_date": "20260113"},
        ]
        expected = [
            "20251129",
            "20251130",
            "20251201",
            "20251219",
            "20260105",
            "20260106",
            "20260107",
            "20260108",
            "20260109",
            "20260110",
            "20260111",
        ]
        output = scrubber.missing_days(inputs)
        assert output == expected

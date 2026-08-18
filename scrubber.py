#!/usr/bin/env python

"""Basic scrubber for IMAP data archive."""

import collections
import csv
import datetime
import os.path
import typing

import imap_data_access
import numpy


def yearmonth_iterator() -> list[tuple[int, int]]:
    """Return (yyyy, mm) pairs for entire history of mission."""
    start = (2025, 9)
    now = datetime.datetime.now(datetime.UTC)
    end = (now.year, now.month)
    return [
        (year, month)
        for year in range(start[0], end[0] + 1)
        for month in range(
            start[1] if year == start[0] else 1, end[1] + 1 if year == end[0] else 13
        )
    ]


# What we probably really want here is a function that takes an
# imap_data_access query and automatically breaks the start/end range
# into chunks, then concatenates the output. Project for later.
def all_latest_files() -> dict[str, list[dict[str, typing.Any]]]:
    """Return latest version of all files, all instruments, all dates."""
    return {
        inst: [
            fileinfo
            for yyyy, mm in yearmonth_iterator()
            for fileinfo in imap_data_access.query(
                instrument=inst,
                start_date=f"{yyyy}{mm:02}01",
                end_date=(
                    datetime.datetime(yyyy + int(mm == 12), mm % 12 + 1, 1)  # noqa: PLR2004
                    - datetime.timedelta(days=1)
                ).strftime("%Y%m%d"),
                version="latest",
            )
        ]
        for inst in imap_data_access.VALID_INSTRUMENTS
        if inst not in ("ialirt", "spacecraft")
    }


def break_by_logical_source(
    fileinfo: list[dict[str, str]],
) -> dict[tuple[str, str, str], list[dict[str, typing.Any]]]:
    """Break results of imap_data_access_query into dict by logical source."""
    fileinfo.sort(key=lambda x: os.path.basename(x["file_path"]))

    def source(x: dict[str, typing.Any]) -> tuple[str, str, str]:
        return (x["instrument"], x["data_level"], x["descriptor"])

    by_source = collections.defaultdict(list)
    for this_file in fileinfo:
        by_source[source(this_file)].append(this_file)
    return by_source


def missing_repoints(fileinfo: list[dict[str, typing.Any]]) -> list[int]:
    """Find missing repointings in a set of files."""
    repoints = [f["repointing"] for f in fileinfo]
    repoints.sort()
    start_idx = numpy.nonzero(numpy.diff(repoints) > 1)[0]
    return [
        repoint
        for i in start_idx
        for repoint in range(repoints[i] + 1, repoints[i + 1])
    ]


def missing_days(fileinfo: list[dict[str, typing.Any]]) -> list[str]:
    """Find missing dates in a set of files."""
    dates = [datetime.datetime.strptime(f["start_date"], "%Y%m%d") for f in fileinfo]
    start_idx = numpy.nonzero(numpy.diff(dates) > datetime.timedelta(days=1))[0]  # type: ignore[arg-type]
    missing = [
        dates[i] + datetime.timedelta(days=days_off)
        for i in start_idx
        for days_off in range(1, (dates[i + 1] - dates[i]).days)
    ]
    return [d.strftime("%Y%m%d") for d in missing]


def combine_missing(
    missing: dict[tuple[str, str, str], list[typing.Any]],
) -> dict[tuple[str, str, str], list[typing.Any]]:
    """Combine missing elements that are common in more than one list."""
    # keyed by instrument, level, descriptor
    result = {}
    inst_levels = set(k[:2] for k in missing.keys())
    for inst_level in inst_levels:  # descriptor roll-up
        in_all = set.intersection(
            *[set(v) for k, v in missing.items() if k[:2] == inst_level]
        )
        result.update(
            {
                k: [i for i in v if i not in in_all]
                for k, v in missing.items()
                if k[:2] == inst_level
            }
        )
        result[(*inst_level, "all")] = sorted(in_all)
    insts = set(k[0] for k in result.keys())
    for inst in insts:  # level roll-up
        in_all = set.intersection(
            *[set(v) for k, v in result.items() if k[0] == inst and k[2] == "all"]
        )
        result.update(
            {
                k: [i for i in v if i not in in_all]
                for k, v in result.items()
                if k[0] == inst and k[2] == "all"
            }
        )
        result[(inst, "all", "all")] = sorted(in_all)
    # instrument roll-up
    in_all = set.intersection(
        *[set(v) for k, v in result.items() if k[1:] == ("all", "all")]
    )
    result.update(
        {
            k: [i for i in v if i not in in_all]
            for k, v in result.items()
            if k[1:] == ("all", "all")
        }
    )
    result[("all", "all", "all")] = sorted(in_all)
    result = {k: v for k, v in result.items() if v}
    return result


def scrubber() -> None:
    """Scrub the archive (main function); check missing days/repoints."""
    latest = {
        logical_source: files
        for inst, inst_files in all_latest_files().items()
        for logical_source, files in break_by_logical_source(inst_files).items()
        if logical_source[1][:2] != "l0"
    }
    latest_repoint_files = {
        k: v for k, v in latest.items() if v and v[0].get("repointing") is not None
    }
    missing = {k: missing_repoints(v) for k, v in latest_repoint_files.items()}
    missing = combine_missing(missing)
    with open("missing_repoints.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Instrument", "Level", "Descriptor", "Repoint"])
        for k in sorted(missing):
            for repoint in missing[k]:
                writer.writerow([*list(k), repoint])
    daily_instruments = ("codice", "hit", "mag", "swapi", "swe")
    latest_daily_files = {k: v for k, v in latest.items() if k[0] in daily_instruments}
    missing = {k: missing_days(v) for k, v in latest_daily_files.items()}  # type: ignore[misc]
    missing = combine_missing(missing)
    with open("missing_days.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Instrument", "Level", "Descriptor", "Date"])
        for k in sorted(missing):
            for dt in missing[k]:
                writer.writerow([*list(k), dt])


if __name__ == "__main__":
    scrubber()

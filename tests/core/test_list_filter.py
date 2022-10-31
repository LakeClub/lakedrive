import re
import json
import random
import pytest

from datetime import datetime, timedelta
from faker import Faker
from typing import Tuple, List, Any

from lakedrive.core.list_filter import (
    TIME_UNITS,
    BYTE_UNITS,
    split_by_comma,
    split_key_value,
)
from lakedrive.core.objects import FileObject
from lakedrive.core.list_filter import filter_file_objects

from ..helpers.generate_files import generate_file_objects_random
from ..helpers.generate_objects import (
    FilenameConfig,
    GenerateFileConfig,
    generate_file_objects,
)


@pytest.fixture(autouse=True)  # type: ignore[misc]
def patch_date_fixed(monkeypatch: Any) -> None:
    """Ensure each requested timestamp within tests is exactly the same"""
    mockdate = datetime.now()
    fromts = datetime.fromtimestamp

    class custom_datetime:
        @classmethod
        def now(cls: Any) -> datetime:
            return mockdate

        @classmethod
        def fromtimestamp(cls: Any) -> datetime:
            return fromts(cls)

    monkeypatch.setattr(__name__ + ".datetime", custom_datetime)
    monkeypatch.setattr("lakedrive.core.list_filter.datetime", custom_datetime)


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def search_items() -> Tuple[
    List[Tuple[Tuple[int, ...], List[FileObject]]], List[FileObject]
]:
    generated_size_tuples: List[Tuple[int, int]] = [(0, 9), (10, 1023)]
    for start_value in list(BYTE_UNITS.values())[1:]:
        # units of 1K, =1024
        end_value = start_value * 1024
        mid_value = start_value * 512

        # deduct start_value from last value in range, so in tests a value is
        # guarantueed to be in specified group when earching for larger
        #  byte-units (e.g. 1GB <= value <= 511GB, 512GB <= value <= 1023GB)
        generated_size_tuples.append((start_value, mid_value - start_value))
        generated_size_tuples.append((mid_value, end_value - start_value))

    # generate list of decades from 1970 to 2070
    # generated_time_tuples = (ftime_min, ftime_max)
    # add >7 day buffer window at edge of time-ranges to ensure the weekly
    # tests cant "find" a time that belongs to other range
    edge_buffer = 86400 * (7 + 1)
    generated_time_tuples: List[Tuple[int, int]] = [
        (
            int(datetime(i, 1, 1, 0, 0, 0).timestamp()) + edge_buffer,
            int(datetime(i + 10, 1, 1, 0, 0, 0).timestamp() - 1) - edge_buffer,
        )
        for i in range(1970, 2070, 10)
    ]

    # combine fsize tuple with ftime tuple in to a new tuple,
    # merged_tuples = (fsize_min, fsize_max, ftime_min, ftime_max)
    merged_tuples = [
        t + generated_time_tuples[idx] for idx, t in enumerate(generated_size_tuples)
    ]

    items_all: List[FileObject] = []
    items_grouped: List[Tuple[Tuple[int, ...], List[FileObject]]] = []
    for idx, tup in enumerate(merged_tuples):
        fsize_min, fsize_max, ftime_min, ftime_max = tup
        file_config = GenerateFileConfig(
            directories=[(2, 2), (2, 2)],
            path="",
            files_in_dir=2,
            filesize_kbytes=(fsize_min, fsize_max),
            filetime_mtime=(ftime_min, ftime_max),
            filename_config=FilenameConfig(
                base_minlength=3,
                base_maxlength=3,
                prefix=f"foo_{str(fsize_min)}-{str(fsize_max)}k-",
                infix="",
                postfix=".foo",
            ),
            tags=json.dumps([{"group": idx}]).encode(),
        )

        items: List[FileObject] = generate_file_objects_random([file_config])
        for item in items:
            if item.size > (fsize_max * 1024):
                raise Exception
            if item.size < (fsize_min * 1024):
                raise Exception
        items_grouped.append((tup, items))

        # add items to total
        items_all += items

    # shuffle to ensure items are mixed randomly
    random.shuffle(items_all)
    return items_grouped, items_all


def test_split_by_comma() -> None:
    input_vars_list = [
        ["name=filename", "mtime=10+", "size=10-"],
        ['name="filename,with,comma"', "123=foo", " leadingspace=10"],
        ['name="filename with spaces"', "123==foo", "trailingspace=10 "],
    ]
    for input_vars in input_vars_list:
        input_string = ",".join(input_vars)
        # remove leading and trailing space from expected output_list
        output_list = [kv.strip(" ") for kv in input_vars]
        assert split_by_comma(input_string) == output_list


def test_split_key_value() -> None:
    input_output = {
        "foo=bar": ("foo", "bar"),
        "FOO=bar": ("foo", "bar"),
        "foo=bar=1": ("foo", "bar=1"),
    }
    for input, output in input_output.items():
        assert split_key_value(input) == output


class TestListFilters:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        search_items: Tuple[
            List[Tuple[Tuple[int, ...], List[FileObject]]], List[FileObject]
        ],
    ) -> None:
        self.items_grouped, self.items_all = search_items

    def test_filter_file_objects__mtime(self) -> None:
        current_time = int(datetime.now().timestamp())

        for group_meta, group_objects in self.items_grouped:
            _, _, ftime_min, ftime_max = group_meta
            # get time range based on current time
            # note this leads to negative values for ftimes that exceed current time,
            # effectivily flipping a "+" to a "-", and "-" to "+",
            # the function should be able to process this just as well
            min_seconds_old = current_time - ftime_min  # min. time to be considered
            max_seconds_old = current_time - ftime_max  # max. time to be considered

            # (1) get objects that are minimal (+max_seconds_old) old, then,
            # (2) get object that are max. (-min_seconds_old) old
            found_objects = filter_file_objects(
                self.items_all,
                [
                    {"FilterBy": "mtime", "Value": f"+{str(max_seconds_old)}"},
                    {"FilterBy": "mtime", "Value": f"-{str(min_seconds_old)}"},
                ],
            )
            assert len(found_objects) == len(group_objects)

            # repeat test for every time_unit
            for unit_abbreviation, unit_fullname in TIME_UNITS.items():
                denom = int(timedelta(**{unit_fullname: 1}).total_seconds())
                """to get widest possible search-range, max. needs to be lowered,
                while min. needs to be increased by 1.
                Dont use rounding as this also must work for negative values."""
                min_tu_old = int(min_seconds_old / denom) + 1
                max_tu_old = int(max_seconds_old / denom) - 1

                found_objects = filter_file_objects(
                    self.items_all,
                    [
                        {
                            "FilterBy": "mtime",
                            "Value": f"+{str(max_tu_old)}{unit_abbreviation}",
                        },
                        {
                            "FilterBy": "mtime",
                            "Value": f"-{str(min_tu_old)}{unit_abbreviation}",
                        },
                    ],
                )
                assert len(found_objects) == len(group_objects)

    def test_filter_file_objects__size(self) -> None:
        for group_meta, group_objects in self.items_grouped:
            fsize_min, fsize_max, _, _ = group_meta
            fsize_min_bytes = fsize_min * 1024
            fsize_max_bytes = fsize_max * 1024

            found_objects = filter_file_objects(
                self.items_all,
                [
                    {"FilterBy": "size", "Value": f"+{str(fsize_min_bytes)}"},
                    {"FilterBy": "size", "Value": f"-{str(fsize_max_bytes)}"},
                ],
            )
            assert len(found_objects) == len(group_objects)

            for unit_abbreviation, bytes_per_unit in BYTE_UNITS.items():
                fsize_min_units = int(fsize_min_bytes / bytes_per_unit)
                # skip test if file sizes in this group are to small
                if fsize_min_units < 1:
                    continue
                fsize_max_units = int(fsize_max_bytes / bytes_per_unit)

                found_objects = filter_file_objects(
                    self.items_all,
                    [
                        {
                            "FilterBy": "size",
                            "Value": f"+{str(fsize_min_units)}{unit_abbreviation}",
                        },
                        {
                            "FilterBy": "size",
                            "Value": f"-{str(fsize_max_units)}{unit_abbreviation}",
                        },
                    ],
                )
                assert len(found_objects) == len(group_objects)

    def test_filter_by_name(self) -> None:

        fake = Faker()

        file_paths_a = [fake.file_path(depth=5) for _ in range(100)]
        search_patterns_a = random.sample(
            set(
                [
                    "/".join(fp.split("/")[(i % 4) : (i % 4 + 2)])
                    for i, fp in enumerate(file_paths_a)
                ]
            ),
            20,
        )

        file_paths_b = [
            fake.file_path(depth=(i % 5), absolute=(i % 2 == 0)) for i in range(1000)
        ]
        search_patterns_b = random.sample(
            set(
                [
                    item
                    for sublist in [fp.split("/") for fp in file_paths_b]
                    for item in sublist
                    if item
                ]
            ),
            20,
        )

        search_patterns = [
            (p, re.compile(p)) for p in search_patterns_a + search_patterns_b
        ]

        file_paths = file_paths_a + file_paths_b
        random.shuffle(file_paths)

        pattern_counts = {
            p: len(list(filter(r.search, file_paths))) for p, r in search_patterns
        }

        file_objects = generate_file_objects(file_paths)
        for pattern, count in pattern_counts.items():
            found_objects = filter_file_objects(
                file_objects,
                [
                    {
                        "FilterBy": "name",
                        "Value": pattern,
                    }
                ],
            )
            assert len(found_objects) == count

    def test_filter_by_invalid(self) -> None:
        with pytest.raises(Exception) as error:
            filter_file_objects(
                self.items_all,
                [
                    {
                        "FilterBy": "invalid",
                        "Value": "nan",
                    }
                ],
            )
        assert str(error.value) == 'FilterBy function "invalid" does not exist'

    def test_filter_novalue(self) -> None:
        with pytest.raises(Exception) as error:
            filter_file_objects(
                self.items_all,
                [
                    {
                        "FilterBy": "name",
                    }
                ],
            )
        assert str(error.value) == 'Value not set for FilterBy function "name"'

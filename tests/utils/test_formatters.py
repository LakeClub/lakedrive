from datetime import datetime

from lakedrive.utils.formatters import time_human_readable, bytes_human_readable


def test_time_human_readable() -> None:
    datetime_str = "2021-11-08T23:34:48"
    timestamp = int(datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S").timestamp())
    assert time_human_readable(timestamp) == datetime_str


def test_bytes_human_readable() -> None:
    expected_values = {
        1: "1",
        1023: "1023",
        1024: "1.0k",
        7750: "7.6k",
        77500: "75.7k",
        856000: "835.9k",
        4020000: "3.8M",
        55612345: "53.0M",
        123456789: "117.7M",
        2345678901: "2.2G",
        34567890123: "32.2G",
        456789012345: "425.4G",
        6789012345678: "6.2T",
        91234567890123: "83.0T",
        4567890123456789: "4.1P",
        123456789012345678: "109.7P",
        91234567891234567890: "79.1E",
        12345678901234567890123: "10.5Z",
        456789012345678901234567890: "Inf",
    }

    for size_in, size_out in expected_values.items():
        assert bytes_human_readable(size_in) == size_out

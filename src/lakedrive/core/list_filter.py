import re
import logging
import operator

from datetime import datetime, timedelta
from typing import List, Tuple, Dict

from ..core.objects import FileObject


logger = logging.getLogger(__name__)


def split_by_comma(line: str) -> List[str]:
    """Split a string by comma into a list,
    - exclude comma between double-quotes
    - remove any leading and trailing spaces"""
    return [kv.strip(" ") for kv in re.split(',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)]


def split_key_value(kv_string: str) -> Tuple[str, str]:
    """Split a string like key=value into (key, value) tuple
    - ensure key is always lowercased
    - value can contain the "=" char, i.e. only split on first occurrence of "="
    """
    key, value = kv_string.split("=", 1)
    return (key.lower(), value)


BYTE_UNITS = {
    "b": 1,
    "k": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
}

COMPARE_OPERATORS = {
    "+": operator.ge,
    "-": operator.le,
}

# follow pattern used by GNU date
TIME_UNITS = {
    "S": "seconds",
    "M": "minutes",
    "H": "hours",
    "d": "days",
    "w": "weeks",
}


def filter_by_name(file_objects: List[FileObject], search_str: str) -> List[FileObject]:
    pattern = re.compile(search_str)
    return [obj for obj in file_objects if pattern.search(obj.name)]


def filter_by_size(file_objects: List[FileObject], value_str: str) -> List[FileObject]:
    unit_key = value_str[-1]
    operator_key = value_str[0]

    if operator_key in COMPARE_OPERATORS:
        value_str = value_str[1:]
        operator_func = COMPARE_OPERATORS[operator_key]
    else:
        operator_func = operator.eq

    if unit_key in BYTE_UNITS:
        compare_value = int(value_str[0:-1]) * BYTE_UNITS[unit_key]
    else:
        compare_value = int(value_str)

    return [
        obj for obj in file_objects if operator_func(obj.size, compare_value) is True
    ]


def filter_by_mtime(file_objects: List[FileObject], value_str: str) -> List[FileObject]:
    unit_key = value_str[-1]
    operator_key = value_str[0]

    if operator_key in COMPARE_OPERATORS:
        value_str = value_str[1:]
        operator_func = COMPARE_OPERATORS[operator_key]
    else:
        operator_func = operator.le

    if unit_key in TIME_UNITS:
        diff_value = int(value_str[0:-1])
        time_unit = TIME_UNITS[unit_key]
    else:
        diff_value = int(value_str)
        time_unit = "seconds"

    point_in_time = int(
        (datetime.now() - timedelta(**{time_unit: diff_value})).timestamp()
    )
    return [
        obj for obj in file_objects if operator_func(point_in_time, obj.mtime) is True
    ]


FILTER_FUNCTIONS = {
    "name": filter_by_name,
    "size": filter_by_size,
    "mtime": filter_by_mtime,
}


def filter_file_objects(
    file_objects: List[FileObject],
    filters: List[Dict[str, str]],
) -> List[FileObject]:
    for filter in filters:
        filter_function_name = filter.get("FilterBy", "")
        try:
            filter_function = FILTER_FUNCTIONS[filter_function_name]
        except KeyError:
            raise Exception(
                f'FilterBy function "{filter_function_name}" does not exist'
            )
        try:
            filter_value_str = filter["Value"]
        except KeyError:
            raise Exception(
                f'Value not set for FilterBy function "{filter_function_name}"'
            )

        file_objects = filter_function(file_objects, filter_value_str)
    return file_objects

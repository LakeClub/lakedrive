import string
import random
import hashlib

from datetime import datetime
from typing import Callable, List, Any

from lakedrive.core.objects import FileObject


def splitlist(input_list: List[Any], no_lists: int = 2) -> List[List[Any]]:
    """Split a list into N new lists"""
    # round-up number of items per list to ensure no_lists is not exceeded
    items_per_list = (len(input_list) + no_lists - 1) // no_lists
    return [
        input_list[i : i + items_per_list]
        for i in range(0, len(input_list), items_per_list)
    ]


def compute_md5sum(file_bytes: bytes) -> str:
    """Compute md5sum from array of bytes"""
    return hashlib.md5(file_bytes).hexdigest()


def utc_offset() -> int:
    """Calculate offset in seconds to UTC time
    positive means ahead of UTC time, negative means behind UTC time"""
    return round((datetime.now() - datetime.utcnow()).total_seconds())


def filter_regfiles(file_objects: List[FileObject]) -> List[FileObject]:
    return [fo for fo in file_objects if fo.name[-1] != "/"]


def argparse_validate_args(main_func: Callable[..., Any], args: List[str]) -> int:
    """Test argparser for a given main function. Return True if valid args are
    passed. Return False if invalid args are passed"""
    try:
        return_code = main_func(args)
        assert isinstance(return_code, int)
        assert return_code >= 0 and return_code <= 255
    except SystemExit as exception:
        # invalid search option -> exit_code == 2
        # --help called -> exit_code == 0
        return exception.code
    except AssertionError:
        # function returns with invalid argument
        return 128
    return return_code


def get_random_string(length: int, special_chars_limited: str = "-_ ") -> str:
    """
    choose from all lowercase letter, special_chars = "!-_.,;:*'() \""

    See:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
        https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata

    """
    safe_chars = string.ascii_letters + string.digits
    tmp_string = "".join(
        [random.choice(safe_chars + special_chars_limited) for i in range(length)]
    )
    # ensure first and last character is a safe char
    # dont do this using strip(), we also need to guarantuee at least 1 char is returned
    if tmp_string[0] in special_chars_limited:
        tmp_string = random.choice(safe_chars) + tmp_string
    if tmp_string[-1] in special_chars_limited:
        tmp_string += random.choice(safe_chars)
    return tmp_string


def flatten_list(outer_list: List[Any]) -> List[Any]:
    return [item for sublist in outer_list for item in sublist]

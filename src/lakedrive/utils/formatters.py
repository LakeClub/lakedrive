from datetime import datetime

from ..core.objects import FileObject


def time_human_readable(epoch_time: int) -> str:
    return datetime.fromtimestamp(epoch_time).isoformat(timespec="seconds")


def bytes_human_readable(size: int) -> str:

    exponent = 1
    symbol = "k"

    if size < 1024:
        return str(size)
    elif size < 1024**2:
        pass  # use default
    elif size < 1024**3:
        exponent, symbol = 2, "M"
    elif size < 1024**4:
        exponent, symbol = 3, "G"
    elif size < 1024**5:
        exponent, symbol = 4, "T"
    elif size < 1024**6:
        exponent, symbol = 5, "P"
    elif size < 1024**7:
        exponent, symbol = 6, "E"
    elif size < 1024**8:
        exponent, symbol = 7, "Z"
    else:
        return "Inf"

    return f"{float(size / 1024**exponent):.1f}{symbol}"


def outputs_file_object(fo: FileObject) -> str:
    return f"{bytes_human_readable(fo.size).rjust(8)} \
{time_human_readable(fo.mtime)} {fo.name}"

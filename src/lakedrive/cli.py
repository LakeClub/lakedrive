import os
import sys
import logging
import argparse

from typing import List, Dict, NoReturn

from lakedrive import get, put, sync_paths, delete
from lakedrive.api import FileBatch, Response
from lakedrive.utils.formatters import outputs_file_object

from .about import __version__, __version_released__

logger = logging.getLogger(__name__)


def configure_logger(name: str, debug: bool = False) -> None:

    _logger = logging.getLogger(name)

    FORMATTER = logging.Formatter(
        "%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - "
        "%(funcName)s - %(message)s"
    )
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(FORMATTER)

    # clear any existing handler(s) before adding new
    for handler in _logger.handlers:
        _logger.removeHandler(handler)
    _logger.addHandler(log_handler)

    if debug is True:
        _logger.setLevel(logging.DEBUG)
    else:
        _logger.setLevel(logging.WARNING)


def cli_stderr_response_string(cli_error_message: str, response: Response) -> str:
    error_categories = {
        400: "",
        403: "PermissionError",
        404: "FileNotFoundError",
    }
    response_error_code = response.status_code
    response_error_message = response.error_message or ""

    error_message = f"lakedrive: {cli_error_message}: "

    error_category = error_categories.get(response_error_code, "")

    if error_category and error_category != response_error_message:
        # prefix category on original message
        error_message += f"{error_category}; {response_error_message}"
    else:
        error_message += response_error_message

    return f"{error_message}\n"


class CustomArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        sys.stderr.write(f"error: {message}\n")
        sys.stderr.write(self.format_help())
        sys.exit(2)


def main(cli_args: List[str]) -> int:
    """Lakedrive CLI"""
    module_name = __name__.split(".", 1)[0]

    configure_logger(name=module_name, debug=(os.environ.get("DEBUG", "") != ""))

    parser = CustomArgParser(prog=module_name)

    parser.add_argument(
        "-V", "--version", action="store_true", help="show version and exit"
    )

    subparsers = parser.add_subparsers(help="<sub-command> help", dest="command")

    methods = {}

    # python -m lakedrive {method} {source} {destination}
    for method in ["Copy", "Sync"]:
        sub = subparsers.add_parser(method.lower(), help=f"Show help for {method}")
        sub.add_argument("source", type=str, help="Source Target")
        sub.add_argument("destination", type=str, help="Destination Target")
        methods[method] = sub

    # python -m lakedrive {method} {target} {regex}
    method = "Find"
    find_options = ["name", "size", "mtime"]
    sub = subparsers.add_parser(method.lower(), help=f"Show help for {method}")
    sub.add_argument("target", type=str, help="Target")
    for opt in find_options:
        sub.add_argument(
            f"-{opt[0]}",
            f"--{opt}",
            type=str,
            nargs="?",
            default="",
            help=f"Filter by {opt}",
        )
    methods[method] = sub

    # python -m lakedrive {method} {target}
    method = "Delete"
    sub = subparsers.add_parser(method.lower(), help=f"Show help for {method}")
    sub.add_argument("target", type=str, help="Target")
    methods[method] = sub

    for argument in ["Copy", "Delete"]:
        methods[argument].add_argument(
            "-r",
            "--recursive",
            action="store_true",
            help=f"{argument} directories recursively",
        )

    # Sync -- extra options based on rsync tool
    parser_sync = methods["Sync"]
    parser_sync.add_argument(
        "--checksum",
        action="store_true",
        help="Skip based on checksum, not mod-time & size",
    )
    parser_sync.add_argument(
        "--delete", action="store_true", help="Delete extraneous items on target"
    )
    parser_sync.add_argument(
        "--force-update",
        action="store_true",
        help="Don't skip files that are newer on the target",
    )
    parser_sync.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a trial run with no changes made",
    )

    args = parser.parse_args(args=cli_args)

    if args.version:
        sys.stdout.write(
            "\n".join(
                [
                    f"{__package__} { __version__ }",
                    f"released: {__version_released__}",
                    "",
                ]
            )
        )
        return 0

    if args.command == "copy":
        source_response = get(args.source, recursive=args.recursive)
        if source_response.status_code != 200 or not isinstance(
            source_response.file_batch, FileBatch
        ):
            error_message = cli_stderr_response_string(
                f"cant fetch from '{args.source}'", source_response
            )
            sys.stderr.write(error_message)
            return 1

        target_response = put(args.destination, source_response.file_batch)
        if target_response.status_code != 201:
            error_message = cli_stderr_response_string(
                "cant write to target", target_response
            )
            sys.stderr.write(error_message)
            return 1

    elif args.command == "sync":
        params: Dict[str, bool] = {
            "checksum": args.checksum,
            "skip_newer_on_target": args.force_update is False,
            "delete_extraneous": args.delete,
            "dry_run": args.dry_run,
        }
        response = sync_paths(args.source, args.destination, params=params)
        if response.status_code != 200:
            error_message = cli_stderr_response_string("sync failed", response)
            sys.stderr.write(error_message)
            return 1

    elif args.command == "find":
        filters = [
            {
                "FilterBy": opt,
                "Value": getattr(args, opt),
            }
            for opt in find_options
            if getattr(args, opt)
        ]
        if args.target[-1] != "/":
            target = f"{args.target}/"
        else:
            target = args.target

        response = get(target, recursive=True, filters=filters)
        if response.status_code != 200 or not isinstance(
            response.file_batch, FileBatch
        ):
            error_message = cli_stderr_response_string("find failed", response)
            sys.stderr.write(error_message)
            return 1

        for fo in response.file_batch.list_objects():
            sys.stdout.write(f"{outputs_file_object(fo)}\n")

        return 0
    elif args.command == "delete":
        target_response = delete(args.target, recursive=args.recursive)
        if target_response.status_code != 204:
            error_message = cli_stderr_response_string("delete failed", target_response)
            sys.stderr.write(error_message)
            return 1
    else:
        sys.stderr.write(parser.format_help())
        return 1

    return 0

import pytest
import logging

from functools import partial

from lakedrive.cli import configure_logger
from lakedrive.cli import main as cli_main

from .helpers.misc import argparse_validate_args


def test_configure_logger(name: str = "dummyLog") -> None:
    configure_logger(name=name, debug=False)
    _logger = logging.getLogger(name)
    assert _logger.level == logging.WARNING
    handlers = _logger.handlers
    assert len(handlers) == 1

    configure_logger(name=name, debug=True)
    _logger = logging.getLogger(name)
    assert _logger.level == logging.DEBUG

    handlers = _logger.handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.StreamHandler)


def test_cli_main(capsys: pytest.CaptureFixture) -> None:
    """check expected exit-/return codes for various arguments"""
    # main() function
    run_main = partial(argparse_validate_args, cli_main)

    # no arguments
    assert run_main([]) == 1

    # bad arguments
    assert run_main(["dummy", "--help"]) == 2

    # help calls
    assert run_main(["--help"]) == 0
    for method in ["copy", "sync", "find", "delete"]:
        assert run_main([method, "--help"]) == 0

    # add this to prevent printing to stderr
    # currently not testing the output of help
    _ = capsys.readouterr()

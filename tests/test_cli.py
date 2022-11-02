import pytest
import logging

from functools import partial

from lakedrive.cli import configure_logger
from lakedrive.cli import main as cli_main
from lakedrive.about import __version__ as version
from lakedrive.about import __version_released__ as version_released

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


class TestCLIMain:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
    ) -> None:
        self.main = partial(argparse_validate_args, cli_main) 

    def test_cli_main(self, capsys: pytest.CaptureFixture) -> None:
        """check expected exit-/return codes for various arguments"""
        # no arguments
        assert self.main([]) == 1

        # bad arguments
        assert self.main(["dummy", "--help"]) == 2

        # help calls
        assert self.main(["--help"]) == 0
        for method in ["copy", "sync", "find", "delete"]:
            assert self.main([method, "--help"]) == 0

        # add this to prevent printing to stderr
        # currently not testing the output of help
        _ = capsys.readouterr()

    def test_cli_version(self, capsys: pytest.CaptureFixture) -> None:
        message = f"lakedrive {version}\nreleased: {version_released}\n"
        for arg in ["--version", "-V"]:
            assert self.main([arg]) == 0
            captured = capsys.readouterr()
            assert captured.out == message

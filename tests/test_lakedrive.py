from lakedrive import __version__
from lakedrive.about import __version__ as cfg_version

def test_version() -> None:
    assert __version__ == cfg_version

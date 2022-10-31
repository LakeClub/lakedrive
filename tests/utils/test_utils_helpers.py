from lakedrive.utils.helpers import compute_md5sum


def test_compute_md5sum() -> None:
    assert compute_md5sum(b"") == "d41d8cd98f00b204e9800998ecf8427e"
    assert compute_md5sum(b"dummy") == "275876e34cf609db118f3d84b799a790"

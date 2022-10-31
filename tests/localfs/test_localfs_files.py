import os

from tempfile import TemporaryDirectory

from lakedrive.localfs.files import remove_file_if_exists


def test_remove_file_if_exists() -> None:

    with TemporaryDirectory() as temp_dir:
        temp_file = f"{temp_dir}/test_remove_file_if_exists"

        # test on non-existent file
        assert os.path.isfile(temp_file) is False
        remove_file_if_exists(temp_file)

        # test on file that exists
        with open(temp_file, "w") as stream:
            stream.write("")
        assert os.path.isfile(temp_file) is True
        remove_file_if_exists(temp_file)
        assert os.path.isfile(temp_file) is False

import os
import pytest

from lakedrive.api import FileBatch, head
from lakedrive.cli import main as cli_main


from typing import Callable, List, Tuple, Optional


def validate_file_single(file_path: str, file_size: int = -1) -> None:

    response = head(file_path)
    assert response.status_code == 200
    assert response.file_count() == 1
    assert isinstance(response.file_batch, FileBatch)

    file_object = response.file_batch.list_objects()[0]
    if file_size >= 0:
        # validate file_size
        assert file_object.size == file_size


class _TestCLICopy:

    source: str
    destination: str
    create_files: Optional[Callable[[str, str, int], List[Tuple[str, int]]]] = None

    def test_file_copy_single(self) -> None:
        assert self.create_files is not None

        source_dir = f"{self.source}/test_file_copy_single"

        # create files at source
        source_filepath, source_filesize = self.create_files(source_dir, "", 1)[0]
        target_filepath = f"{source_filepath}.copy"

        # execute
        cli_args = ["copy", source_filepath, target_filepath]
        return_code = cli_main(cli_args)
        assert return_code == 0

        # test if file is copied
        validate_file_single(target_filepath, source_filesize)

    def test_file_copy_single_to_directory(self) -> None:
        assert self.create_files is not None

        source_dir = f"{self.source}/test_copy_single_to_directory"

        # create files at source
        source_filepath, source_filesize = self.create_files(source_dir, "", 1)[0]
        target_filepath = f"{source_dir}/test_copy_dir"

        # execute
        cli_args = ["copy", source_filepath, f"{target_filepath}/"]
        return_code = cli_main(cli_args)
        assert return_code == 0

        # test if file is copied
        validate_file_single(
            f"{target_filepath}/{os.path.basename(source_filepath)}", source_filesize
        )

    def test_file_copy_invalid_source(self, capsys: pytest.CaptureFixture) -> None:
        source_filepath = f"{self.source}/test_invalid_source_file"
        target_filepath = f"{source_filepath}.copy"

        # execute
        cli_args = ["copy", source_filepath, f"{target_filepath}/"]
        return_code = cli_main(cli_args)
        captured = capsys.readouterr()
        assert return_code == 1
        assert (
            captured.err
            == f"lakedrive: cant fetch from \
'{source_filepath}': FileNotFoundError\n"
        )

    def test_file_copy_invalid_target(self, capsys: pytest.CaptureFixture) -> None:
        assert self.create_files is not None

        source_dir = f"{self.source}/test_copy_invalid_target"

        # create files at source
        source_filepath, _ = self.create_files(source_dir, "", 1)[0]
        target_filepath = source_filepath

        # execute
        cli_args = ["copy", source_filepath, f"{target_filepath}/"]
        return_code = cli_main(cli_args)
        captured = capsys.readouterr()
        assert return_code == 1
        assert (
            captured.err
            == f"lakedrive: cant write to target: \
path '{target_filepath}' taken by non-directory\n"
        )

    def test_file_copy_multi(self) -> None:
        """Test copying ~5 files in directory"""
        pass

    def test_file_copy_recursive(self) -> None:
        """Test copying ~5 files in directory-in-directory"""
        pass


class _TestCLISync:
    source: str
    destination: str
    create_files: Optional[Callable[[str, str, int], List[Tuple[str, int]]]] = None

    def test_sync_directory_empty(self) -> None:
        assert self.create_files is not None

        source_dir = f"{self.source}/test_sync_empty_directory"

        # create empty source directory
        self.create_files(source_dir, "", 0)

        # define destination and ensure if non-exist
        target_dir = f"{self.source}/test_sync_destination_dir"
        assert head(target_dir, recursive=True).status_code == 404

        cli_args = ["sync", source_dir, target_dir]
        return_code = cli_main(cli_args)
        assert return_code == 0

        # destination should be created
        assert head(target_dir, recursive=True).status_code == 200

    def test_sync_directory_source_not_exist(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        assert self.create_files is not None

        source_dir = f"{self.source}/test_sync_directory_source_not_exist"
        cli_args = ["sync", source_dir, f"{source_dir}_dummy"]
        return_code = cli_main(cli_args)
        captured = capsys.readouterr()
        assert return_code == 1
        assert (
            captured.err
            == f"lakedrive: sync failed: \
FileNotFoundError; '{source_dir}' not exists\n"
        )


class _TestCLIFind:
    target: str
    create_files: Optional[Callable[[str, str, int], List[Tuple[str, int]]]] = None

    def test_find_directory_not_exists(self, capsys: pytest.CaptureFixture) -> None:
        target_dir = f"{self.target}/test_find_directory_not_exist"
        # test for both non and "/" postfixed directories, result should be same
        for pf in ["", "/"]:
            cli_args = ["find", f"{target_dir}{pf}"]
            return_code = cli_main(cli_args)
            captured = capsys.readouterr()
            assert return_code == 1
            assert (
                captured.err
                == f"lakedrive: find failed: \
FileNotFoundError; '{target_dir}' not exists\n"
            )

    def test_find_directory(self, capsys: pytest.CaptureFixture) -> None:
        assert self.create_files is not None

        target_dir = f"{self.target}/test_find_directory_with_files"

        no_test_files = 3
        files_created = self.create_files(target_dir, "foo", no_test_files)
        files_created_bn = [
            (os.path.basename(fname), fsize) for fname, fsize in files_created
        ]

        cli_args = ["find", target_dir]
        return_code = cli_main(cli_args)
        captured = capsys.readouterr()
        assert return_code == 0
        assert captured.err == ""

        printed_lines = captured.out.split("\n")

        # output ends with a new-line, so last field in erry should be empty
        assert printed_lines[-1] == ""
        assert len(printed_lines[:-1]) == no_test_files

        found_files: List[Tuple[str, int]] = []
        for line in printed_lines[:-1]:
            fsize, _, fname = line.strip(" ").split(" ", 2)
            found_files.append((fname, int(fsize)))

        assert sorted(found_files) == sorted(files_created_bn)


class _TestCLIDelete:

    source: str
    create_files: Optional[Callable[[str, str, int], List[Tuple[str, int]]]] = None

    def test_file_delete_single(self, capsys: pytest.CaptureFixture) -> None:
        assert self.create_files is not None
        source_dir = f"{self.source}/test_file_delete_single"
        source_filepath, _ = self.create_files(source_dir, "", 1)[0]

        cli_args = ["delete", source_filepath]
        return_code = cli_main(cli_args)
        assert return_code == 0

        # check file no longer exists
        response = head(source_filepath)
        assert response.status_code == 404

    def test_file_delete_single_error(self, capsys: pytest.CaptureFixture) -> None:
        assert self.create_files is not None
        filepath = f"{self.source}/test_dir_not_exists/test_file_not_exists"

        cli_args = ["delete", filepath]
        return_code = cli_main(cli_args)
        assert return_code == 1

        captured = capsys.readouterr()
        assert (
            captured.err
            == f"lakedrive: delete failed: FileNotFoundError; \
'{os.path.dirname(filepath)}' not exists\n"
        )

    def test_file_delete_multi(self) -> None:
        """Test deleting ~5 files in directory"""
        pass

    def test_file_delete_recursive(self) -> None:
        """Test deleting ~5 files in directory-in-directory"""
        pass

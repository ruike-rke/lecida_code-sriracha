# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Test for Sriracha remote utils."""

from hashlib import sha1
from pathlib import Path
from typing import Callable, Dict, Optional, Sequence, Set

import pytest

import sriracha.remote as remote
from sriracha.main import Config

# TODO: Test rerun of s3_to_local with different values of `sync`


TEST_BUCKET = 'lecida-test-bucket'
TEST_DIR = 'circleci/sriracha/remote/v2'
TEST_PATH = f's3://{TEST_BUCKET}/{TEST_DIR}'

FILE_HASHES = {
    '1.abc.def': 'a4f3c4f6fb6ac5ffffe009c9d26a33c875d240f3',
    'empty_file': 'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    '.updsasd': '182370b4007fc1b39424f53112be962bf0d9d5a6',
    'folder/123': 'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    'folder/987': 'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    'folder.manifest': 'a852db4db68bb42ec01d35714ccfd4c299948d0e'
}
DIR_NAMES = ('folder',)


InvPReason = remote.InvalidS3Path.Reason
IncludePatterns = Optional[Sequence[str]]
GetDownloadPath = Callable[[str, IncludePatterns], Path]


@pytest.fixture
def get_download_path(mocked_config: Config) -> GetDownloadPath:
    """Fixture that downloads a file/directory and return its path.

    Args:
        mocked_config: The mocked configuration.

    """
    def fn(rel_path: str, include_patterns: IncludePatterns = None)\
            -> Path:
        """Download a file/directory and return its path.

        Args:
            rel_path: The relative path of the file/directory.
            include_patterns: Argument passed to s3_to_local.

        Returns:
            The path of the sync object.

        """
        dst_path = Path(remote.s3_to_local(s3_path=f'{TEST_PATH}/{rel_path}',
                                           include_patterns=include_patterns))

        assert mocked_config.local_sync_dir is not None
        assert dst_path == (mocked_config.local_sync_dir / TEST_BUCKET
                            / TEST_DIR / rel_path)
        return dst_path

    return fn


@pytest.mark.parametrize(
    ('path', 'reason'),
    (('http://www.lecida.com/sriracha', InvPReason.WRONG_SCHEME),
     (f'{TEST_PATH}abc', InvPReason.NO_OBJECT_FOUND),
     (f'{TEST_PATH}abc/', InvPReason.NO_OBJECT_FOUND),
     (f'{TEST_PATH}/abc', InvPReason.NO_OBJECT_FOUND),
     (f'{TEST_PATH}/abc/', InvPReason.NO_OBJECT_FOUND),
     ('s3://', InvPReason.NO_BUCKET_NAME),
     ('s3://hand-lecida-test-bucket-does-not-exist', InvPReason.NO_SUCH_BUCKET),
     ('s3://hand-lecida-test-bucket-does-not-exist/abc', InvPReason.NO_SUCH_BUCKET),
     ('s3://./', InvPReason.INVALID_BUCKET_NAME),
     ('http://www.lecida.com/sriracha', InvPReason.WRONG_SCHEME))
)
def test_invalid_path(path: str, reason: InvPReason) -> None:
    """Test for an invalid path. It should raise an exception.

    Args:
        path: The remote path.
        reason: The expected reason for the exception.

    """
    with pytest.raises(remote.InvalidS3Path) as e:
        remote.s3_to_local(path)
    assert e.value.reason == reason


def _get_file_hash(path: Path) -> str:
    return sha1(path.read_bytes()).hexdigest()  # noqa: S303


# TODO: Test download_mode
@pytest.mark.parametrize(('rel_path', 'file_hash'),
                         ((f'full/{file_name}', file_hash)
                          for file_name, file_hash in FILE_HASHES.items()))
@pytest.mark.parametrize('include_patterns',
                         (None, (), ('*.def', 'empty_file')))
def test_download_file(get_download_path: GetDownloadPath, rel_path: str,
                       include_patterns: IncludePatterns, file_hash: str)\
        -> None:
    """Test downloading a file.

    Args:
        get_download_path: The path download fixture.
        include_patterns: Argument passed to s3_to_local.
        file_hash: The expected hash of the file.

    """
    def get_path():
        return get_download_path(rel_path, include_patterns)

    if include_patterns is not None:
        with pytest.raises(ValueError):
            get_path()
        return

    downloaded_path = get_path()
    assert downloaded_path.is_file()
    assert _get_file_hash(downloaded_path) == file_hash


# TODO: Test download_mode, include_patterns
@pytest.mark.parametrize('rel_path', ('full',))
@pytest.mark.parametrize('file_hashes', (FILE_HASHES,))
@pytest.mark.parametrize('dir_names', (DIR_NAMES,))
@pytest.mark.parametrize('include_patterns', (None,))
def test_download_dir(get_download_path: GetDownloadPath, rel_path: str,
                      include_patterns: IncludePatterns,
                      file_hashes: Dict[str, str],
                      dir_names: Set[str]) -> None:
    """Test syncing a directory.

    Args:
        get_download_path: The path download fixture.
        include_patterns: Argument passed to s3_to_local.
        file_hashes: The expected {file name → file relative path} dictionary
            for the requested directory.
        dir_names: The expected set of subdirectory names for the requested
            directory.

    """
    downloaded_path = get_download_path(rel_path, include_patterns)
    assert downloaded_path.is_dir()

    files_and_dirs = list(downloaded_path.glob('**/*'))
    assert len(files_and_dirs) == len(file_hashes) + len(dir_names)
    for path in files_and_dirs:
        file_rel_path = str(path.relative_to(downloaded_path))
        if path.is_file():
            file_hash = file_hashes[file_rel_path]
            assert _get_file_hash(path) == file_hash
        else:
            assert file_rel_path in dir_names

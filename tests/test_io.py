# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Test for Sriracha IO utils."""

import subprocess
from pathlib import Path
from typing import Dict, Set

import pytest

import sriracha.io as sio


@pytest.fixture
def example_src_subdirs(tmp_path: Path) -> Set[Path]:
    """Return a set of example subdirectories."""
    src_dir = tmp_path / 'src_dir'
    return {src_dir, src_dir / 'subdir'}


@pytest.fixture
def example_src_files(tmp_path: Path) -> Dict[Path, str]:
    """Return a set of example file paths and their expected content."""
    src_dir = tmp_path / 'src_dir'
    return {
        (src_dir / 'a.csv'): 'a,b,c\n1,2,3\nx,y,z\n',
        (src_dir / 'subdir/a.csv'): 'Lecida',
    }


@pytest.fixture
def populated_src_dir(tmp_path: Path, example_src_subdirs: Set[Path],
                      example_src_files: Dict[Path, str]) -> Path:
    """Return the path to a populated source directory."""
    for dir_path in example_src_subdirs:
        dir_path.mkdir(exist_ok=True, parents=True)

    for file_path, content in example_src_files.items():
        file_path.write_text(data=content)

    return tmp_path / 'src_dir'


def test_gzip_file_src_does_not_exist(tmp_path: Path) -> None:
    """Try gzipping a file that does not exist."""
    with pytest.raises(ValueError):
        sio.gzip_file(filepath=tmp_path / 'does_not_exist.abc')


def test_gzip_file_dst_already_exists(populated_src_dir: Path,
                                      example_src_files: Dict[Path, str]) \
        -> None:
    """Try gzipping a file when the destination already exists."""
    for src_path in example_src_files:
        dst_path = src_path.with_suffix(f'{src_path.suffix}.gz')
        dst_path.touch()
        with pytest.raises(FileExistsError):
            sio.gzip_file(filepath=src_path)


def test_gzip_file_dst_not_a_file(populated_src_dir: Path,
                                  example_src_subdirs: Set[Path]) -> None:
    """Try gzipping a directory with the gzip_file function."""
    for subdir in example_src_subdirs:
        with pytest.raises(ValueError):
            sio.gzip_file(filepath=subdir)


def test_gzip_file_valid(populated_src_dir: Path,
                         example_src_files: Dict[Path, str]) -> None:
    """Try gzipping valid files."""
    for src_path, content in example_src_files.items():
        dst_path = src_path.with_suffix(f'{src_path.suffix}.gz')
        sio.gzip_file(filepath=src_path)
        assert dst_path.exists()

        # noqa: S603
        p = subprocess.run(args=('gzip', '-c', '-d', str(dst_path)),
                           check=True, stdout=subprocess.PIPE)

        assert p.stdout.decode('utf-8') == content


def test_gzip_dir_does_not_exist(tmp_path: Path) -> None:
    """Try gzipping a directory that does not exist."""
    with pytest.raises(ValueError):
        sio.gzip_directory(dirpath=tmp_path / 'does_not_exist')


def test_gzip_dir_already_exists(populated_src_dir: Path) -> None:
    """Try gzipping a directory that already exists."""
    dst_path = populated_src_dir.with_suffix(f'.tar.gz')
    dst_path.touch()
    with pytest.raises(FileExistsError):
        sio.gzip_directory(dirpath=populated_src_dir)


def test_gzip_dir_dst_not_a_dir(populated_src_dir: Path,
                                example_src_files: Dict[Path, str]) -> None:
    """Try gzipping a file with the gzip_directory function."""
    for src_path in example_src_files:
        with pytest.raises(ValueError):
            sio.gzip_directory(dirpath=src_path)


def test_gzip_dir_valid(tmp_path: Path,
                        example_src_subdirs: Set[Path],
                        example_src_files: Dict[Path, str],
                        populated_src_dir: Path) -> None:
    """Try gzipping valid directories."""
    dst_path = populated_src_dir.with_suffix('.tar.gz')

    sio.gzip_directory(dirpath=populated_src_dir)
    assert dst_path.exists()

    p = subprocess.run(args=('tar', 'tf', str(dst_path)), check=True,
                       stdout=subprocess.PIPE)

    files_and_dirs = {Path(u) for u in p.stdout.decode('utf-8').split('\n')}
    expected_files_and_dirs = {
        *(path.relative_to(tmp_path)
          for path in (set(example_src_files) | example_src_subdirs)),
        Path('.')
    }
    assert files_and_dirs == expected_files_and_dirs

    for src_file, content in example_src_files.items():
        rel_path = src_file.relative_to(tmp_path)
        p = subprocess.run(args=('tar', 'xfO', str(dst_path), str(rel_path)),
                           check=True, stdout=subprocess.PIPE)
        assert p.stdout.decode('utf-8') == content

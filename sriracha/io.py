# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""IO utility functions."""

import filecmp
import gzip
import logging
import os
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Sequence, Union, overload

import pandas as pd

from sriracha.time import get_timerange_indices

logger = logging.getLogger(__name__)


def gzip_file(filepath: Union[str, Path]) -> None:
    """Compress a file.

    Args:
        filepath: The path to the file.

    """
    src_path = Path(filepath)
    if not src_path.is_file():
        raise ValueError(f'Called gzip_file, but {src_path} is not a file')

    dst_path = src_path.with_suffix(suffix=f'{src_path.suffix}.gz')
    if dst_path.exists():
        raise FileExistsError(f'{dst_path} already exists.')

    with src_path.open(mode='rb') as f_in:
        with gzip.open(filename=dst_path, mode='wb') as f_out:
            shutil.copyfileobj(fsrc=f_in, fdst=f_out)


def gzip_directory(dirpath: Union[str, Path]) -> None:
    """Compress a directory.

    Args:
        dirpath: The path to the directory.

    """
    src_path = Path(dirpath)
    if not src_path.is_dir():
        raise ValueError(f'Called gzip_directory, but {src_path} is not a '
                         'directory')

    dst_path = src_path.with_suffix(suffix=f'.tar.gz')
    if dst_path.exists():
        raise FileExistsError(f'{dst_path} already exists.')

    with tarfile.open(name=dst_path, mode='w:gz') as tar:
        tar.add(str(src_path), arcname=src_path.name)


def linecount(filename: str) -> int:
    """Count the number of lines in a file.

    Args:
        filename: The path to the file.

    Returns:
        The result of wc -l, i.e. the count of line.

    """
    p = subprocess.run(['wc', '-l', filename], check=True,  # noqa: S603,S607
                       stdout=subprocess.PIPE)
    return int(p.stdout.strip().partition(b' ')[0])


@overload
def read_csv_part(csv_path: str, start_index: int, end_index: int,  # noqa: # D103
                  timestamps: None) -> pd.DataFrame:
    ...


@overload
def read_csv_part(csv_path: str, start_index: pd.Timestamp,  # noqa: # D103
                  end_index: pd.Timestamp,
                  timestamps: Sequence[pd.Timestamp]) -> pd.DataFrame:
    ...


def read_csv_part(csv_path, start_index, end_index, timestamps=None):
    """Read part of a csv.

    Assumes header is 1 line long. Supports integer and timestamp indices.

    """
    if ((type(start_index) is pd.Timestamp)
            and (type(end_index) is pd.Timestamp)
            and (timestamps is not None)):
        # read the index only first
        start_index, end_index = get_timerange_indices(timestamps,
                                                       start_index,
                                                       end_index)
    elif not (isinstance(start_index, int) and isinstance(end_index, int)):
        raise ValueError(
            "Only supports both integer or both datetime indices,"
            "and timestamps must not be None is datetimes provided")
    logger.info(f"Reading {end_index - start_index} rows")
    return pd.read_csv(
        csv_path,
        skiprows=list(range(1, start_index + 1)),
        nrows=end_index - start_index, memory_map=True), start_index, end_index


def append_to_csv_file(filepath: str, df: pd.DataFrame) -> None:
    """Append a dataframe to filepath if it already exists.

    Otherwise, write the dataframe as CSV.

    Args:
        filepath: The path of the file to append to.
        df: The DataFrame to append.

    """
    if os.path.exists(filepath):
        df_onfile = pd.read_csv(filepath, chunksize=1)
        chunk_onfile = next(df_onfile)
        # check columns match
        if set(chunk_onfile.columns) != set(df.columns):
            raise ValueError("Columns of saved CSV and "
                             "supplied dataframe do not match")
        # enforce same column ordering
        df = df.loc[:, chunk_onfile.columns]
        with open(filepath, 'a') as f:
            df.to_csv(f, index=None, header=None)
    else:
        df.to_csv(filepath, index=None)


def parse_tid_from_key(example_name: str) -> str:
    """Split thing id from the filename with structure.

    File name must be formatted as: tid_starttimesstamp_endtimestamp.

    Args:
        example_name: The example name

    Returns:
        The corresponding thing_id

    """
    return example_name.split('_')[0]


def merge_datasets_helper(in_dir_1: str, in_dir_2: str, out_dir: str,
                          symlink: bool) -> None:
    """Merge 2 datasets by symlinks and appending their summary files.

    Args:
        in_dir_1: The path to the first dataset.
        in_dir_2: The path to the second dataset.
        out_dir: The path to the output dataset.
        symlink: Whether to create symlinks. Otherwise, copy files.

    """
    def disambiguate_and_copy(fn, dir_1, dir_2, dir_out):
        full_f_path_1 = os.path.join(dir_1, fn)
        full_f_path_2 = os.path.join(dir_2, fn)

        # check if the files are the same. if so, don't need to
        # disambiguate
        if filecmp.cmp(full_f_path_1, full_f_path_2, shallow=False):
            new_fn_full_path = os.path.join(out_dir, fn)
            if symlink:
                os.symlink(full_f_path_1, new_fn_full_path)
            else:
                shutil.copyfile(full_f_path_1, new_fn_full_path)
        else:
            basename, ext = os.path.splitext(fn)
            new_fn_1 = f"{basename}_1{ext}"
            new_fn_2 = f"{basename}_2{ext}"
            new_fn_1_abspath = os.path.join(dir_out, new_fn_1)
            new_fn_2_abspath = os.path.join(dir_out, new_fn_2)

            if symlink:
                os.symlink(full_f_path_1, new_fn_1_abspath)
                os.symlink(full_f_path_2, new_fn_2_abspath)
            else:
                shutil.copyfile(full_f_path_1, new_fn_1_abspath)
                shutil.copyfile(full_f_path_2, new_fn_2_abspath)

    def merge_csv_files(csv_file_list, out):
        header_saved = False
        with open(out, 'w') as fout:
            for filename in csv_file_list:
                with open(filename) as fin:
                    header = next(fin)
                    if not header_saved:
                        fout.write(header)
                        header_saved = True
                    for line in fin:
                        fout.write(line)

    in_dir_1 = os.path.abspath(in_dir_1)
    in_dir_2 = os.path.abspath(in_dir_2)
    os.makedirs(out_dir, exist_ok=False)

    # copy the exclusive files in top level
    in_dir_1_files = {f for f in os.listdir(in_dir_1)
                      if os.path.isfile(os.path.join(in_dir_1, f))}
    in_dir_2_files = {f for f in os.listdir(in_dir_2)
                      if os.path.isfile(os.path.join(in_dir_2, f))}
    for fname in in_dir_1_files.symmetric_difference(in_dir_2_files):
        new_full_f_path = os.path.join(out_dir, fname)
        if fname in in_dir_1_files:
            full_f_path = os.path.join(in_dir_1, fname)
        else:
            full_f_path = os.path.join(in_dir_2, fname)
        if symlink:
            os.symlink(full_f_path, new_full_f_path)
        else:
            shutil.copyfile(full_f_path, new_full_f_path)

    # deal with same-name files in top level
    for fname in in_dir_1_files.intersection(in_dir_2_files):
        # if file exists in other folder
        # we have to merge the files if they
        # are csv, otherwise we disambiguate by appending 1 and 2
        if fname.endswith('.csv'):
            full_path_1 = os.path.join(in_dir_1, fname)
            full_path_2 = os.path.join(in_dir_2, fname)
            new_full_path = os.path.join(out_dir, fname)
            merge_csv_files([full_path_1, full_path_2], new_full_path)
        else:
            # disambiguate and copy
            disambiguate_and_copy(fname, in_dir_1, in_dir_2, out_dir)

    # symlink the exclusive directories
    in_dir_1_dirs = {f for f in os.listdir(in_dir_1)
                     if os.path.isdir(os.path.join(in_dir_1, f))}
    in_dir_2_dirs = {f for f in os.listdir(in_dir_2)
                     if os.path.isdir(os.path.join(in_dir_2, f))}
    for dname in in_dir_1_dirs.symmetric_difference(in_dir_2_dirs):
        new_full_dname_path = os.path.join(out_dir, dname)
        if dname in in_dir_1_dirs:
            full_f_path = os.path.join(in_dir_1, dname)
        else:
            full_f_path = os.path.join(in_dir_2, dname)
        os.symlink(full_f_path, new_full_dname_path)

    # recurse into directories, but with symlink on
    for dname in in_dir_1_dirs.intersection(in_dir_2_dirs):
        merge_datasets_helper(
            os.path.join(in_dir_1, dname),
            os.path.join(in_dir_2, dname),
            os.path.join(out_dir, dname),
            symlink=True)


def merge_datasets(in_dir_1: str, in_dir_2: str, out_dir: str) -> None:
    """Merge 2 datasets by symlinks and appending their summary files.

    Args:
        in_dir_1: The path to the first dataset.
        in_dir_2: The path to the second dataset.
        out_dir: The path to the output dataset.

    """
    merge_datasets_helper(in_dir_1, in_dir_2, out_dir, symlink=False)

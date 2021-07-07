# Copyright (C) 2019 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Test for Sriracha CSV utils."""

from pathlib import Path
from typing import Iterable, List

import pytest

import sriracha.csv as scsv


def test_multiple_csv_writes(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_row(a=2, b=3, c='a')

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_row(b=6, a=2, c=5)

    file_content = csv_path.read_text()
    assert file_content == 'a,b,c\n2,3,a\n2,6,5\n'


def test_write_different_field_names(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_row(a=2, b=3, c=4)

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        with pytest.raises(ValueError):
            csv_writer.write_row(b=6, a=2, c=5, d=6.1)


def test_different_field_names_than_existing(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_row(a=2, b=3, c=4)

    with pytest.raises(ValueError):
        with scsv.CSVWriter(path=csv_path, field_names=('a', 'c', 'b')):
            pass


def test_custom_field_names(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with scsv.CSVWriter(path=csv_path, field_names=('a', 'c', 'b')) \
            as csv_writer:
        csv_writer.write_row(b=2, a=3, c=4)

    file_content = csv_path.read_text()
    assert file_content == 'a,c,b\n3,4,2\n'


def test_invalid_string_name(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with pytest.raises(TypeError):
        with scsv.CSVWriter(path=csv_path, field_names=('a', 2, 'b')):  # type: ignore  # noqa: E501
            pass


def test_field_name_ordering_fn(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    def order_fn(it: Iterable[str]) -> List[str]:
        return sorted(it, reverse=True)

    with scsv.CSVWriter(path=csv_path, field_name_order_fn=order_fn) \
            as csv_writer:
        csv_writer.write_row(b=2, a=3, c=4)

    file_content = csv_path.read_text()
    assert file_content == 'c,b,a\n4,2,3\n'


def test_write_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_rows(rows=({'a': i, 'b': 3 * i} for i in (1, 3)))

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_row(b=7, a='hello')

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_rows(rows=[])

    file_content = csv_path.read_text()
    assert file_content == 'a,b\n1,3\n3,9\nhello,7\n'


def test_write_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / 'test.csv'

    with scsv.CSVWriter(path=csv_path) as csv_writer:
        csv_writer.write_columns(a=[2, 3, 4], b=['c', 6, 4])

    file_content = csv_path.read_text()
    assert file_content == 'a,b\n2,c\n3,6\n4,4\n'

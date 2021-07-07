# Copyright (C) 2019 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""CSV utility functions."""
from __future__ import annotations

import csv
import io
from pathlib import Path
from types import TracebackType
from typing import (Any, Callable, Dict, Iterable, List, Optional, TextIO,
                    Type, Union)


class lecida_dialect(csv.unix_dialect):  # noqa: N801
    """CSV dialect for Lecida projects."""

    quoting = csv.QUOTE_MINIMAL
    strict = True


csv.register_dialect('lecida', lecida_dialect)


class CSVWriter(object):
    """Custom CSV writer, which allows appending to CSV files."""

    def __init__(self, path: Union[str, Path],
                 field_names: Optional[Iterable[str]] = None,
                 field_name_order_fn:
                 Optional[Callable[[Iterable[str]], List[str]]] = None,
                 use_line_buffering: bool = True) \
            -> None:
        """Initialize the CSV writer.

        Args:
            path: The path to a new or existing CSV.
            field_names: A list of field names. If None, it will be
                automatically generated before writing the first row. Defaults
                to None
            field_name_order_fn: An ordering function for the field names. If
                None, field_names will be used, or the ordering of the first
                row if field_names is None as well. Defaults to None.
            use_line_buffering: whether to use a line-by-line buffering

        """
        self._path = Path(path)

        # Set field names
        self._field_names: Optional[List[str]]
        if field_names is not None:
            if field_name_order_fn is not None:
                raise ValueError('field_names and field_name_order_fn can\'t '
                                 'be both specified.')
            self._field_names = list(field_names)
        else:
            self._field_names = None

        # If the file already exists, get field names from the file
        if self._path.exists():
            with self._path.open(newline='') as csv_file:
                csv_reader = csv.DictReader(f=csv_file, dialect='lecida')
                try:
                    next(csv_reader)
                    if self._field_names is not None:
                        # Check that the field names match
                        if list(csv_reader.fieldnames) != self._field_names:
                            raise ValueError(f'Custom field_names '
                                             f'{self._field_names} do not '
                                             f'correspond to the existing '
                                             f'file\'s field names '
                                             f'{csv_reader.fieldnames}')
                    else:
                        self._field_names = list(csv_reader.fieldnames)
                    self._file_already_had_header = True
                except StopIteration:
                    self._file_already_had_header = False
        else:
            self._file_already_had_header = False

        if (self._field_names is not None
                and not all(isinstance(fn, str) for fn in self._field_names)):
            raise TypeError(f'field_names should contain only strings, but '
                            f'got {self._field_names}')

        self._field_name_order_fn = field_name_order_fn
        self._file: Optional[TextIO] = None
        self._dict_writer: Optional[csv.DictWriter] = None

        self._use_line_buffering = use_line_buffering

    def __enter__(self) -> CSVWriter:
        """Enter the context manager, just returning self."""
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> Optional[bool]:
        """Exit the context manager, closing open file."""
        if self._file is not None:
            self._file.close()

        return None

    def _get_dict_writer(self, row: Dict[str, Any]) -> csv.DictWriter:
        if self._field_names is None:
            if self._field_name_order_fn is not None:
                self._field_names = self._field_name_order_fn(row.keys())
            else:
                self._field_names = list(row.keys())

        buffering = 1 if self._use_line_buffering else io.DEFAULT_BUFFER_SIZE
        self._file = self._path.open('a', newline='', buffering=buffering)

        dict_writer = csv.DictWriter(f=self._file, dialect='lecida',
                                     fieldnames=self._field_names)
        if not self._file_already_had_header:
            dict_writer.writeheader()

        return dict_writer

    def write_row(self, **row: Any) -> None:
        """Write a row to the csv file."""
        if self._dict_writer is None:
            self._dict_writer = self._get_dict_writer(row=row)

        self._dict_writer.writerow(rowdict=row)

    def write_rows(self, rows: Iterable[Any]) -> None:
        """Write multiple rows to the csv file."""
        if self._dict_writer is None:
            rows_it = iter(rows)
            try:
                first_row = next(rows_it)
            except StopIteration:
                # Nothing to do
                return
            self._dict_writer = self._get_dict_writer(row=first_row)
            self._dict_writer.writerow(rowdict=first_row)
            self._dict_writer.writerows(rows_it)
        else:
            self._dict_writer.writerows(rowdicts=rows)

    def write_columns(self, **columns: Iterable[Any]) -> None:
        """Write multiple columnss to the csv file."""
        rows = (dict(zip(columns, row_values))
                for row_values in zip(*columns.values()))
        self.write_rows(rows=rows)

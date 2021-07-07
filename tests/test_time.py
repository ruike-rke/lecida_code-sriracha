# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Test for Sriracha time utils."""

import datetime

import numpy as np
import pandas as pd
import pytest

import sriracha.time as time_utils


@pytest.fixture()
def timestamps() -> pd.Series:
    return pd.Series(pd.to_datetime(np.arange(17)
                                    * datetime.timedelta(1).total_seconds()
                                    * 1e9))


@pytest.fixture()
def seq() -> pd.Series:
    return pd.Series([0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1])


@pytest.mark.parametrize('input_timedelta',
                         (pytest.param(datetime.timedelta(1), id='datetime'),
                          pytest.param(np.timedelta64(1, 'D'), id='numpy'),
                          pytest.param(pd.Timedelta('1D'), id='pandas')))
def test_timedelta_to_days(input_timedelta: time_utils.TDType) -> None:
    # test datetime.timedelta
    delta_days = time_utils.timedelta_to_days(input_timedelta)
    assert delta_days == 1.0


def test_timedelta_arr_to_days(timestamps: pd.Series) -> None:
    # pandas series
    td_arr = timestamps[1:] - timestamps[:-1].values
    delta_days_arr = time_utils.timedelta_arr_to_days(td_arr)
    assert np.all(delta_days_arr == 1.0)

    # numpy datetime64 array
    delta_days_arr = time_utils.timedelta_arr_to_days(td_arr.values)
    assert np.all(delta_days_arr == 1.0)

    # array of datetimes
    timestamps = [datetime.timedelta(1)] * 10
    delta_days_arr = time_utils.timedelta_arr_to_days(timestamps)
    assert np.all(delta_days_arr == 1.0)


@pytest.mark.parametrize('query_start,query_end',
                         (pytest.param(datetime.datetime(1970, 1, 1),
                                       datetime.datetime(1970, 1, 5),
                                       id='datetime'),
                          pytest.param(np.datetime64('1970-01-01'),
                                       np.datetime64('1970-01-05'),
                                       id='numpy'),
                          pytest.param(pd.to_datetime('1970-01-01'),
                                       pd.to_datetime('1970-01-05'),
                                       id='pandas')))
@pytest.mark.parametrize('numpy_mode', (True, False))
def test_get_timerange_indices(timestamps: pd.Series,
                               numpy_mode: bool,
                               query_start: time_utils.TSType,
                               query_end: time_utils.TSType) -> None:

    ts = timestamps.values if numpy_mode else timestamps
    # numpy timestamps
    start_idx, end_idx = time_utils.get_timerange_indices(ts, query_start,
                                                          query_end)
    assert start_idx == 0
    assert end_idx == 5


@pytest.mark.parametrize('segment_mode', ('pandas', 'numpy', 'list'))
def test_get_segments(seq: pd.Series, segment_mode: str) -> None:
    if segment_mode == 'numpy':
        sequence = seq.values
    elif segment_mode == 'list':
        sequence = seq.values.tolist()
    else:
        assert segment_mode == 'pandas'
        sequence = seq
    # test pandas series
    segments = time_utils.get_segments(sequence)
    assert ({'start_index', 'end_index', 'index_length'}
            <= set(segments.columns))
    # test exclusive index
    assert np.all(
        segments['index_length'].values
        == (segments['end_index'].values - segments['start_index'].values)
    )


@pytest.mark.parametrize('timestamp_mode', ('pandas', 'numpy', 'list'))
def test_days_since_epoch(timestamps: pd.Series, timestamp_mode: str):
    if timestamp_mode == 'numpy':
        ts = timestamps.values
    elif timestamp_mode == 'list':
        ts = list(timestamps.values)
    else:
        assert timestamp_mode == 'pandas'
        ts = timestamps
    true_dse = np.arange(17)
    dse = time_utils.days_since_epoch(ts)
    assert np.all(true_dse == dse)

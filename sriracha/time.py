# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Time utilities."""

import itertools
import logging
from datetime import datetime, timedelta
from typing import Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

TSType = Union[pd.Timestamp, str, np.datetime64, datetime]
TDType = Union[pd.Timedelta, str, np.timedelta64, timedelta]


def time_to_string(ts: TSType) -> str:
    """Convert a timestamp to string format: %Y-%m-%dT%H-%M-%S-%f."""
    return f'{pd.Timestamp(ts):%Y-%m-%dT%H-%M-%S-%f}'


def time_to_isoformat(ts: TSType) -> str:
    """Convert a timestamp to ISO format."""
    return pd.Timestamp(ts).to_pydatetime().isoformat()


def is_overlapping(segs: pd.DataFrame, start: TSType, end: TSType) \
        -> pd.Series:
    """Return a boolean array indicating boolean segments.

    Args:
        segs: DataFrame of segment endpoints with columns 'start_timestamp' and
            'end_timestamp'
        start: start timestamp for query segment
        end: end timestamp for query segment

    Returns:
        A series of boolean.

    """
    return ~((segs.end_timestamp < start) | (segs.start_timestamp > end))


def timedelta_to_days(td: TDType) -> float:
    """Convert a timedelta to days.

    Args:
        td: The timedelta to convert.

    Returns:
        The number of days in this timedelta.

    """
    return pd.Timedelta(td) / pd.Timedelta(days=1)


def timedelta_arr_to_days(td_arr: Sequence[TDType]) -> np.ndarray:
    """Convert an array of timedelta to days.

    Args:
        td_arr: The array of timedeltas to convert.

    Returns:
        An array containing the number of days in each timedelta.

    """
    return (pd.Series(td_arr, dtype=np.dtype('timedelta64[ns]'))
            / pd.Timedelta(days=1)).values


def get_timerange_indices(timestamps: Union[pd.Series, np.ndarray],
                          start_time: TSType, end_time: TSType) \
        -> Tuple[int, int]:
    """Get indices that correspond to a time range.

    Args:
        timestamps: The timestamps.
        start_time: The beginning of the time range.
        end_time: The end of the time range.

    Returns:
        The start and end indices for the timerange.

    """
    start_time, end_time = np.datetime64(start_time), np.datetime64(end_time)

    if isinstance(timestamps, pd.Series):
        # Converting to NumPy
        timestamps = timestamps.values

    if len(timestamps.shape) > 1:
        timestamps = np.squeeze(timestamps, axis=1)
    start_index = np.searchsorted(timestamps, start_time, side='left')
    end_index = np.searchsorted(timestamps, end_time, side='right')
    return start_index, end_index


def get_segments(errors: Sequence[float],
                 timestamps: Optional[Sequence[TSType]] = None,
                 ignore_value: float = 0.) -> pd.DataFrame:
    """Get segment information from a 1-D array.

    The powerhouse of this module. Segments are defined as contiguous runs of
    some particular value.

    Args:
        errors: The sequence of errors.
        timestamps: The sequence of timestamps for each error. If not None,
            will be used to generate the start and end timestamp (and duration)
            of each segment.
        ignore_value: The "zero" value.

    Returns:
        The DataFrame of segments.

    """
    sparse_index = pd.SparseArray(errors, fill_value=ignore_value).sp_index
    sparse_block_index = sparse_index.to_block_index()

    start_indices = sparse_block_index.blocs
    # end_indices are NOT inclusive
    end_indices = sparse_block_index.blocs + sparse_block_index.blengths
    items = [
        ("start_index", start_indices),
        ("end_index", end_indices),
        ("index_length", sparse_block_index.blengths),
    ]

    if timestamps is not None:
        timestamps_arr = np.asarray(timestamps, dtype='datetime64[ns]')
        start_timestamps = timestamps_arr[start_indices]
        # end timestamps ARE inclusive
        end_timestamps = timestamps_arr[end_indices - 1]
        delta_days = timedelta_arr_to_days(end_timestamps - start_timestamps)
        items.extend([
            ("start_timestamp", start_timestamps),
            ("end_timestamp", end_timestamps),
            ("delta_days", delta_days),
        ])

    df = pd.DataFrame({k: v for k, v in items}, columns=[k for k, v in items])

    return df


def coalesce_segments(segments_df: pd.DataFrame,
                      coalesce_interval_days: float = 0.25) -> pd.DataFrame:
    """Coalesce segments that are coalesce_interval (days) from each other.

    Args:
        segments_df: The segment DataFrame, as generate with get_segments with
            timestamps != None.
        coalesce_interval_days: The day interval threshold for coalescing.
            Defaults to 0.25.

    Returns:
        The coalesced segment dataframe.

    """
    if segments_df.shape[0] < 2:
        return segments_df

    start_timestamps = segments_df['start_timestamp']
    end_timestamps = segments_df['end_timestamp']
    diffs = start_timestamps[1:] - end_timestamps[:-1].values
    diffs.name = None

    # convert to timedelta
    coalesce_interval_td = timedelta(days=coalesce_interval_days)
    mask = (diffs < coalesce_interval_td)
    coalesced_segments = get_segments(mask, None, False)

    start_indices = list(coalesced_segments['start_index'])
    # Here we add 1 because we take segments of diffs
    # which tell us that the end index is included
    end_indices = list(coalesced_segments['start_index']
                       + coalesced_segments['index_length'] + 1)
    # add back the segments that are far away from others
    segments_covered = set(itertools.chain.from_iterable(
        (range(a, b) for a, b in zip(start_indices, end_indices))))
    additional_segment_idxs = np.asarray(
        [i for i in range(segments_df.shape[0]) if i not in segments_covered])
    start_indices_arr = np.asarray(sorted(start_indices
                                          + list(additional_segment_idxs)))
    end_indices_arr = np.asarray(sorted(end_indices
                                        + list(additional_segment_idxs + 1)))

    new_start_indices = (segments_df['start_index'].iloc[start_indices_arr]
                         .values)
    new_end_indices = segments_df['end_index'].iloc[end_indices_arr - 1].values
    new_lengths = new_end_indices - new_start_indices
    if np.any(new_lengths == 0):
        logger.warning("There are some start_index==end_index in given "
                       "segments to coalesce, which may cause problems.")
    items = [
        ("start_index", new_start_indices),
        ("end_index", new_end_indices),
        ("index_length", new_lengths),
    ]

    start_timestamps = start_timestamps.values[start_indices_arr]
    end_timestamps = end_timestamps.values[end_indices_arr - 1]
    delta_days = timedelta_arr_to_days(end_timestamps - start_timestamps)
    items.extend([
        ("start_timestamp", start_timestamps),
        ("end_timestamp", end_timestamps),
        ("delta_days", delta_days),
    ])
    df = pd.DataFrame({k: v for k, v in items}, columns=[k for k, v in items])
    return df


def days_since_epoch(timestamps: Union[pd.Series, np.ndarray]) -> np.ndarray:
    """Return the number of days since 1970/01/01 for an array of timestamps.

    Args:
        timestamps: The array of timestamps.

    Returns:
        The array of days-since-epoch.

    """
    return timedelta_arr_to_days(timestamps
                                 - np.datetime64(pd.datetime(1970, 1, 1)))

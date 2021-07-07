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

import datetime
import logging

import pytest
from freezegun import freeze_time

import sriracha.logging as slogging
from sriracha.main import Config


@pytest.mark.parametrize('now', (datetime.datetime(2018, 10, 17, 8, 0, 23),))
@pytest.mark.parametrize('project', ('sriracha/test',))
@pytest.mark.parametrize('log_file_prefix', ('log',))
@pytest.mark.parametrize('timespec', ('seconds', 'hours'))
def test_file_handler(mocked_config: Config, now: datetime.datetime,
                      project: str, log_file_prefix: str, timespec: str)\
        -> None:
    """Test syncing a directory.

    Args:
        downloaded_path: The path of the downloaded file.=
        sync: Argument passed to s3_to_local.
        file_hashes: The expected {file name → file relative path} dictionary
            for the requested directory.
        dir_names: The expected set of subdirectory names for the requested
            directory.

    """
    with freeze_time(time_to_freeze=now):
        file_handler = slogging.get_log_file_handler(
            project=project, log_file_prefix=log_file_prefix,
            timespec=timespec, verbose=False
        )

    test_warning = 'This is a test warning.'
    test_error = 'This is an error.'

    logger = logging.getLogger('test_logger')
    logger.handlers = [file_handler]
    logger.warning(test_warning)
    logger.error(test_error)

    assert mocked_config.log_dir is not None
    test_dir = mocked_config.log_dir / project

    log_files = list(test_dir.glob('*'))
    assert len(log_files) == 1

    log_file, = log_files

    prefix = f'{log_file_prefix}_'
    suffix = '.log'

    assert log_file.name.startswith(prefix)
    assert log_file.name.endswith(suffix)

    assert (log_file.name[len(prefix):-len(suffix)]
            == now.isoformat(timespec=timespec).replace(':', ''))

    assert log_file.read_text() == f'{test_warning}\n{test_error}\n'

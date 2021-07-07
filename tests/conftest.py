# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""pytest utility functions for Sriracha."""

import pytest

import sriracha.main


@pytest.fixture(autouse=True)
def mocked_config(monkeypatch, tmp_path_factory) -> sriracha.main.Config:
    """Mock a configuration using temporary directories."""
    config = sriracha.main.Config(
        local_sync_dir=tmp_path_factory.mktemp(basename='local_sync_dir'),
        log_dir=tmp_path_factory.mktemp(basename='log_dir'),
        circleci_api_token=None
    )

    monkeypatch.setattr(sriracha.main, 'get_config', lambda: config)
    return config

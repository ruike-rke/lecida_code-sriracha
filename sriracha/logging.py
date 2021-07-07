# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Logging Utilities."""

import datetime
import logging
import sys
from typing import Optional

import click

import sriracha.main


def get_log_file_handler(project: str, log_file_prefix: str = 'log',
                         timespec: str = 'seconds', verbose: bool = True)\
        -> logging.FileHandler:
    """Obtain a log file handler to use in Lecida projects.

    Args:
        project: The arbitrary project name, e.g. 'learn/opm',
            'evonik/parser'. If possible, it should start with
            '{git_repo_name}/'.
        log_file_prefix: The prefix for the log files. Defaults to 'log'.
        timespec: The timespec for the datetime in the log file name.
        verbose: Verbose output.

    Returns:
        A logging file handler.

    """
    config = sriracha.main.get_config()

    if config.log_dir is None:
        raise ValueError('Please run "sriracha configure" to configure '
                         'the Lecida log directory.')

    dt = datetime.datetime.now().isoformat(timespec=timespec).replace(':', '')
    log_dir = config.log_dir / project

    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / f'{log_file_prefix}_{dt}.log'

    if verbose:
        click.secho(f'Writing to log file {log_path}', err=True, fg='green')

    return logging.FileHandler(filename=str(log_path), mode='a')


def setup_logging(project: str, level: int = logging.INFO,
                  log_file_prefix: str = 'log', timespec: str = 'seconds',
                  verbose: bool = True, format_: Optional[str] = None,
                  style: Optional[str] = None,
                  datefmt: str = '%Y-%m-%dT%H:%M:%S%z') -> None:
    """Set up logging with a stderr stream + a file stream.

    Args:
        project: The arbitrary project name, e.g. 'learn/opm',
            'evonik/parser'. If possible, it should start with
            '{git_repo_name}/'.
        level: The default logging level to use.
        log_file_prefix: The prefix for the log files. Defaults to 'log'.
        timespec: The timespec for the datetime in the log file name.
        verbose: Whether to print information about the log file path.
        format_: Format string to use for the handler. If None, defaults to
            "{asctime} {levelname:8} {message}". format can only be None if
            style is None.
        style: Type of format string. Can be "%", "{", "$". If None, defaults
            to "{". style can only be None if format is None.
        datefmt: The datetime format.

    """
    stream_handler = logging.StreamHandler(stream=sys.stderr)
    file_handler = get_log_file_handler(project=project,
                                        log_file_prefix=log_file_prefix,
                                        timespec=timespec, verbose=verbose)
    handlers = (stream_handler, file_handler)

    if format_ is None:
        if style is not None:
            raise ValueError('`format_` is specified, but not `style`.')
        format_ = '{asctime} {levelname:8} {message}'
        style = '{'
    elif style is None:
        raise ValueError('`style` is specified, but not `format_`.')

    logging.basicConfig(handlers=handlers, level=level, format=format_,
                        style=style, datefmt=datefmt)

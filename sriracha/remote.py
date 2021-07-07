# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Remote utilities."""
from __future__ import annotations

import enum
import io
import logging
from contextlib import redirect_stderr
from pathlib import Path
from typing import Optional, Sequence, Union
from urllib.parse import urlparse

import boto3
import botocore.exceptions
from awscli.clidriver import create_clidriver
from typing_extensions import Literal

import sriracha.main

logger = logging.getLogger(__name__)


@enum.unique
class DownloadMode(enum.IntEnum):
    """Possible modes for downloading files using s3_to_local.

    See the documentation of `s3_to_local` for a detailed description for these
    values.

    """

    ALWAYS_DOWNLOAD = 0
    FILE_DOES_NOT_EXIST = 1
    SIZE_ONLY = 2
    SIZE_AND_TIMESTAMP = 3
    NEVER_DOWNLOAD = 4


class InvalidS3Path(Exception):
    """Exception raised when a given S3 path is invalid."""

    class Reason(str, enum.Enum):
        """Reason for the exception."""

        NO_OBJECT_FOUND = 'No object found'
        NO_BUCKET_NAME = 'No bucket name'
        NO_SUCH_BUCKET = 'Bucket not found'
        INVALID_BUCKET_NAME = 'Invalid bucket name'
        WRONG_SCHEME = 'Wrong scheme, the path must start with s3://'

    def __init__(self, s3_path: str, reason: InvalidS3Path.Reason) -> None:
        """Initialize the exception.

        Args:
            s3_path: The S3 path we tried to download.
            reason: The reason why the S3 path is invalid.

        """
        super().__init__(f'Invalid path "{s3_path}": {reason.value}.')
        self._reason = reason

    @property
    def reason(self) -> InvalidS3Path.Reason:
        """Return the reason for the exception."""
        return self._reason


class AWSCLIException(Exception):
    """Exception raised by an AWS CLI command."""

    def __init__(self, args: Sequence[str], error_code: int,
                 stderr: str) -> None:
        """Initialize the exception.

        Args:
            args: The AWS CLI command arguments.
            error_code: The error code returned.
            stderr: The content of stderr.

        """
        args_str = ' '.join(args)
        super().__init__(f'Command aws {args_str} returned error code '
                         f'{error_code}, with the following stderr:\n{stderr}')
        self.error_code = error_code
        self.stderr = stderr


def run_aws_cli_command(*args: str):
    """Return an AWS CLI command, and return an exception if needed."""
    cli_driver = create_clidriver()

    f_stderr = io.StringIO()
    with redirect_stderr(f_stderr):
        return_code = cli_driver.main(args=args)

    stderr = f_stderr.getvalue()
    f_stderr.close()

    if return_code:
        raise AWSCLIException(args=args, error_code=return_code, stderr=stderr)


def s3_to_local(s3_path: str,
                sync: Union[bool, None, Literal['if_not_exists']] = None,
                download_mode: DownloadMode = DownloadMode.SIZE_AND_TIMESTAMP,
                include_patterns: Optional[Sequence[str]] = None) -> str:
    """Convert an s3 path to the local version.

    Use the local sync directory configured before. Triggers a aws sync call to
    ensure consistency between S3 and local path if s3_path is a directory in
    s3. If it is a file, it will download the file using boto3.

    Args:
        s3_path: Path which begins with 's3://'
        sync: Sync the contents of local path with remote. Deprecated, use
            download_mode instead.
        download_mode: The download decision behavior. Could be one of these
            enum values:
            - ALWAYS_DOWNLOAD: Always download, even if the destination file
                already exists. Not valid when syncing directories.
            - FILE_DOES_NOT_EXIST: Only download if the destination file does
                not exist. Not valid when syncing directories.
            - SIZE_ONLY: Do not download if the destination file exists with
                the same size as the source file.
            - SIZE_AND_TIMESTAMP: Do not download if the destination file
                exists with the same size and the same modification timestamp
                as the source file.
            - NEVER_DOWNLOAD: Never download the files, only return the local
                path corresponding to the S3 remote path (hopefully previously
                downloaded).
            Defaults to SIZE_AND_TIMESTAMP.
        include_patterns: A list of patterns to include. If specified, only
            files that match one of these patterns will be synced. Defaults to
            None.

    Returns:
        The local path.

    """
    if download_mode not in DownloadMode:
        raise ValueError(f'Wrong value for download_mode: {download_mode}')

    parsed = urlparse(s3_path)
    if parsed.scheme == '':
        if parsed.netloc != '':
            raise ValueError(f"{s3_path} must be S3 path or local path")
        return s3_path

    config = sriracha.main.get_config()
    local_sync_dir = config.local_sync_dir
    if local_sync_dir is None:
        raise ValueError("Please run 'sriracha configure' to configure "
                         "local sync directory")

    s3_relpath = Path(parsed.netloc, parsed.path.lstrip('/'))
    local_path = local_sync_dir / s3_relpath

    if sync:
        if sync is True:
            download_mode = DownloadMode.ALWAYS_DOWNLOAD
        elif sync == 'if_not_exists':
            download_mode = DownloadMode.FILE_DOES_NOT_EXIST
        elif sync is False:
            download_mode = DownloadMode.NEVER_DOWNLOAD
        else:
            raise ValueError(f'Wrong value for sync: {sync}.')
        logger.warning('The use of sync is deprecated and will be removed in '
                       'the future. Use download_mode instead.')

    if download_mode != DownloadMode.NEVER_DOWNLOAD:
        client = boto3.client("s3")
        url = urlparse(s3_path)

        if url.scheme != 's3':
            raise InvalidS3Path(s3_path=s3_path,
                                reason=InvalidS3Path.Reason.WRONG_SCHEME)

        if not url.netloc:
            raise InvalidS3Path(s3_path=s3_path,
                                reason=InvalidS3Path.Reason.NO_BUCKET_NAME)
        bucket = url.netloc
        key = url.path.strip('/')

        if not key:
            # S3 path is just the bucket name (i.e. no key passed)
            _download_s3_dir(local_path, s3_path, download_mode,
                             include_patterns)

        try:
            # Try to get object metadata - if successful we have a valid file.
            client.head_object(Bucket=bucket, Key=key)

            if include_patterns is not None:
                raise ValueError('include_patterns are only allowed for directories.')  # noqa: E501

            _download_s3_file(local_path, s3_path, download_mode)
        except botocore.exceptions.ClientError as e:
            # If we get a 404 back then we're dealing with an S3 directory
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                _download_s3_dir(local_path, s3_path, download_mode,
                                 include_patterns)
            else:
                raise e

    return str(local_path)


def _download_s3_file(
        local_path: Path, s3_path: str,
        download_mode: DownloadMode = DownloadMode.SIZE_AND_TIMESTAMP
) -> None:
    """Download a file from S3 using the boto3 library.

    Args:
        local_path: Directory or file path where the data will reside locally
        s3_path: Path which begins with 's3://'
        download_mode: The download decision behavior. Could be one of these
            enum values:
            - ALWAYS_DOWNLOAD: Always download, even if the destination file
                already exists. Not valid when syncing directories.
            - FILE_DOES_NOT_EXIST: Only download if the destination file does
                not exist. Not valid when syncing directories.
            - SIZE_ONLY: Do not download if the destination file exists with
                the same size as the source file.
            - SIZE_AND_TIMESTAMP: Do not download if the destination file
                exists with the same size and the same modification timestamp
                as the source file.
            - NEVER_DOWNLOAD: Never download the files, only return the local
                path corresponding to the S3 remote path (hopefully previously
                downloaded).
            Defaults to SIZE_AND_TIMESTAMP.

    """
    if (download_mode == DownloadMode.FILE_DOES_NOT_EXIST and local_path.exists()):  # noqa: E501
        return

    s3 = boto3.resource("s3")
    parsed = urlparse(s3_path)
    bucket = s3.Bucket(parsed.netloc)

    # make sure local parent dir is created
    key = parsed.path.strip('/')

    s3_obj = bucket.Object(key=key)

    if (local_path.exists() and download_mode in (DownloadMode.SIZE_ONLY, DownloadMode.SIZE_AND_TIMESTAMP)):  # noqa: E501
        stat = local_path.stat()

        if s3_obj.content_length == stat.st_size:
            if download_mode == DownloadMode.SIZE_ONLY:
                return

            if s3_obj.last_modified.timestamp() == stat.st_mtime:
                return

    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        s3_obj.download_file(str(local_path))
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise InvalidS3Path(
                s3_path=s3_path, reason=InvalidS3Path.Reason.NO_OBJECT_FOUND
            ) from e
        raise e


def _download_s3_dir(
        local_path: Path, s3_path: str,
        download_mode: DownloadMode = DownloadMode.SIZE_AND_TIMESTAMP,
        include_patterns: Optional[Sequence[str]] = None
) -> None:
    """Download a directory from S3 using the AWS CLI s3 cp sync command.

    Args:
        local_path: Directory or file path where the data will reside locally
        s3_path: Path which begins with 's3://'
        download_mode: The download decision behavior. Could be one of these
            enum values:
            - ALWAYS_DOWNLOAD: Always download, even if the destination file
                already exists. Not valid when syncing directories.
            - FILE_DOES_NOT_EXIST: Only download if the destination file does
                not exist. Not valid when syncing directories.
            - SIZE_ONLY: Do not download if the destination file exists with
                the same size as the source file.
            - SIZE_AND_TIMESTAMP: Do not download if the destination file
                exists with the same size and the same modification timestamp
                as the source file.
            - NEVER_DOWNLOAD: Never download the files, only return the local
                path corresponding to the S3 remote path (hopefully previously
                downloaded).
            Defaults to SIZE_AND_TIMESTAMP.
        include_patterns: A list of patterns to include. If specified, only
            files that match one of these patterns will be synced. Defaults to
            None.

    """
    if download_mode in (DownloadMode.ALWAYS_DOWNLOAD,
                         DownloadMode.FILE_DOES_NOT_EXIST):
        logger.warning('Cannot run s3_to_local on a directory with '
                       'the specified download mode. Falling back to '
                       'SIZE_AND_TIMESTAMP download mode.')
        download_mode = DownloadMode.SIZE_AND_TIMESTAMP

    additional_args = []
    if download_mode == DownloadMode.SIZE_ONLY:
        additional_args.append('--size-only')
    elif download_mode == DownloadMode.SIZE_AND_TIMESTAMP:
        additional_args.append('--exact-timestamps')
    else:
        raise ValueError(f'Download mode not understood: {download_mode}')

    try:
        s3 = boto3.resource("s3")
        url = urlparse(s3_path)
        bucket_name = url.netloc
        bucket = s3.Bucket(bucket_name)
        key = url.path.strip('/')
        objects = list(bucket.objects.filter(Prefix=key).limit(1))
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidBucketName':
            raise InvalidS3Path(s3_path=s3_path,
                                reason=InvalidS3Path.Reason.INVALID_BUCKET_NAME)  # noqa: E501
        elif error_code == 'NoSuchBucket':
            raise InvalidS3Path(s3_path=s3_path,
                                reason=InvalidS3Path.Reason.NO_SUCH_BUCKET)
        else:
            raise e

    if len(objects) == 0:
        raise InvalidS3Path(s3_path=s3_path,
                            reason=InvalidS3Path.Reason.NO_OBJECT_FOUND)

    if include_patterns is not None:
        additional_args += ['--exclude', '*']
        for pattern in include_patterns:
            additional_args += ['--include', pattern]

    run_aws_cli_command('s3', 'sync', s3_path, str(local_path), *additional_args)  # noqa: E501

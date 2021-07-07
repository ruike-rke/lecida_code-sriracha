# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Sriracha CLI functions."""

import json
import logging
import stat
from configparser import ConfigParser
from pathlib import Path
from typing import (Callable, Dict, NamedTuple, Optional, TypeVar, Union,
                    get_type_hints)
from urllib.parse import ParseResult, urlparse

import boto3
import click
from botocore.exceptions import ClientError
from typing_extensions import Literal

import sriracha.circleci as scci
import sriracha.remote
from sriracha.remote import DownloadMode

LECIDA_HOME = Path.home() / '.lecida'
LECIDA_CONFIG_PATH = LECIDA_HOME / 'config.ini'

# Only read write permissions for the current user
CONFIG_FILE_PERMISSIONS = stat.S_IRUSR | stat.S_IWUSR

T = TypeVar('T')
TOrEmptyString = Union[Literal[''], T]


logger = logging.getLogger(__name__)


class _ClickContext(NamedTuple):
    lecida_config: ConfigParser


@click.group()
@click.pass_context
def cli(ctx) -> None:
    """Group for click."""
    lecida_config = ConfigParser()

    if LECIDA_CONFIG_PATH.exists():
        lecida_config.read(LECIDA_CONFIG_PATH)

    if 'sriracha' not in lecida_config:
        lecida_config['sriracha'] = {}

    ctx.obj = _ClickContext(lecida_config=lecida_config)


class Config(NamedTuple):
    """User-specific configuration."""

    local_sync_dir: Optional[Path] = None
    log_dir: Optional[Path] = None
    circleci_api_token: Optional[str] = None


def get_config() -> Config:
    """Return the user's config dictionary."""
    if not LECIDA_CONFIG_PATH.exists():
        raise ValueError('Config file does not exist. Please run "sriracha '
                         'configure".')

    lecida_config = ConfigParser()
    lecida_config.read(LECIDA_CONFIG_PATH)

    if 'sriracha' not in lecida_config:
        raise ValueError('"sriracha" section not found. Please run "sriracha '
                         'configure".')

    sriracha_config = lecida_config['sriracha']

    config: Dict[str, Union[int, str, Path]] = {}

    # Values of sriracha_config are string. Cast them if needed
    type_hints = get_type_hints(Config)
    for key, value in sriracha_config.items():
        target_type = type_hints[key]
        if target_type == Optional[Path]:
            config[key] = Path(value).expanduser()
        elif target_type == Optional[int]:
            config[key] = int(value)
        elif target_type == Optional[str]:
            config[key] = value
        else:
            raise TypeError(f'Unknown target type: {target_type}')

    return Config(**config)  # type: ignore


def _get_default_value_from_ctx(key: str, default: TOrEmptyString = '',
                                is_dir: bool = False) \
        -> Callable[[], TOrEmptyString]:
    def fn() -> TOrEmptyString:
        # Get the value from the existing dict, or `default` if the value does
        # not exist.
        obj: _ClickContext = click.get_current_context().obj
        value = obj.lecida_config['sriracha'].get(key, default)

        if value and is_dir:
            # Make sure the dir path is a local path and create the directory
            # before returning its path
            parsed = urlparse(value)
            if parsed.scheme != '' or parsed.netloc != '':
                raise ValueError(f"{key} cannot be a remote path.")

            path = Path(value).expanduser().absolute()
            path.mkdir(parents=True, exist_ok=True)

            return str(path)

        return value

    return fn


@cli.command('configure')
@click.option(
    '--local-sync-dir', '-s', type=click.Path(file_okay=False),
    help='Local sync directory for S3 files.',
    prompt='S3 Local Sync Directory',
    default=_get_default_value_from_ctx(key='local_sync_dir',
                                        default=str(LECIDA_HOME / 's3'),
                                        is_dir=True)
)
@click.option(
    '--log-dir', '-l', type=click.Path(file_okay=False),
    help='Local directory to store logs.', prompt='Local log directory',
    default=_get_default_value_from_ctx(key='log_dir',
                                        default=str(LECIDA_HOME / 'logs'),
                                        is_dir=True)
)
@click.option(
    '--circleci-api-token', '-c', type=str, help='CircleCI Personal Token.',
    prompt='CircleCI Personal Token',
    default=_get_default_value_from_ctx(key='circleci_api_token')
)
@click.pass_obj
def configure(obj: _ClickContext, **kwargs) -> None:
    """Configure local directory path for S3 utilities."""
    config = obj.lecida_config['sriracha']

    if kwargs.keys() != set(Config._fields):
        raise ValueError(
            'Keyword arguments and configuartion fields do not match.'
        )

    for key in Config._fields:
        if kwargs[key] != '':
            # Add non-empty values to the config
            config[key] = kwargs[key]
        elif key in config:
            del config[key]

    # Create the config parent directory
    LECIDA_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Set the correct file permissions
    if LECIDA_CONFIG_PATH.exists():
        if LECIDA_CONFIG_PATH.stat().st_mode != CONFIG_FILE_PERMISSIONS:
            LECIDA_CONFIG_PATH.chmod(mode=CONFIG_FILE_PERMISSIONS)
    else:
        LECIDA_CONFIG_PATH.touch(mode=CONFIG_FILE_PERMISSIONS)

    # Write the config file
    with LECIDA_CONFIG_PATH.open('w') as config_file:
        obj.lecida_config.write(config_file)

    logger.info(f'Wrote to {LECIDA_CONFIG_PATH}')


@cli.command()
@click.argument('project', type=str)
@click.argument('job', type=str)
@click.option('--branch', type=str, default='master', prompt=True,
              help='name of the git branch to use')
@click.option('--revision', type=str, help='git revision')
@click.option('--tag', type=str, help='git tag')
def run_ci(project: str, branch: str, job: str, revision: Optional[str],
           tag: Optional[str]) -> None:
    """Trigger a CircleCI job for a Lecida project."""
    config = get_config()

    if config.circleci_api_token is None:
        raise ValueError('circleci_api_token not configured. Run `sriracha '
                         'configure` to configure')

    resp = scci.trigger_job(api_token=config.circleci_api_token,
                            project=project, branch=branch, job=job,
                            revision=revision, tag=tag)

    click.echo_via_pager(json.dumps(resp, indent=4))


@cli.command()
@click.argument('path', type=str)
def s3_to_local(path: str):
    """Run s3_to_local on the specified S3 path.

    Echoes the local path upon completion.

    [PATH] needs to be an S3 path.

    """
    out_path = sriracha.remote.s3_to_local(
        path, download_mode=DownloadMode.SIZE_AND_TIMESTAMP
    )
    click.echo(out_path)


@cli.command()
@click.argument('path', type=str)
def get_manifest(path: str):
    """Attempt to print the dataset information from the manifest.

    [PATH] needs to be an S3 dataset path.

    """
    base_url = urlparse(path.strip("/"))
    if base_url.scheme != "s3":
        raise click.UsageError(
            f"URL scheme should be s3, but received {base_url.geturl()}"
        )

    s3 = boto3.resource("s3")
    manifest_filenames = ["lecida__manifest.yml", "manifest.yml"]

    def read_s3(base_url: ParseResult, filename: str) -> Optional[bytes]:
        try:
            obj = s3.Object(
                bucket_name=base_url.netloc,
                key=base_url.path.strip("/") + f"/{filename}"
            )
            return obj.get()['Body'].read()
        except ClientError as e:
            # Only allow NoSuchKey errors, blow up on any other errors
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise e

    body: Optional[bytes] = None
    for mf in manifest_filenames:
        body = read_s3(base_url, mf)
        if body is not None:
            break
    if body is None:
        raise click.ClickException(
            f"Can't find any manifest files ({manifest_filenames}) in {path}"
        )

    click.secho(
        f"Found manifest in {base_url.geturl()}/{mf}", fg='green', err=True
    )
    click.echo(body.decode("utf-8"))

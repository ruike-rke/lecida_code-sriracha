# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Tests for YAML utilities."""

from pathlib import Path

import pytest
from yaml.constructor import ConstructorError

import sriracha.yaml as syaml

# TODO: Test that the data is loaded as expected
# TODO: Test dumper + loader = identity


class Base(syaml.YAMLRegistered, register_yaml=False):
    """Base class. Not registered for YAML but its children will be."""

    def __init__(self, arg1, arg2, *args, kwarg1='default', **kwargs):
        """Initialize the instance."""
        pass

    @classmethod
    def check_yaml_tag(cls, subcls, yaml_tag):
        """Check that a YAML tag is correct for the given subclass."""
        if not yaml_tag.endswith('Class'):
            raise ValueError
        super().check_yaml_tag(subcls, yaml_tag)

    @staticmethod
    def format_yaml_tag(subcls):
        """Format a tag for a subclass."""
        return f'!{subcls.__name__}Class'


class SimpleRegisteredClass(syaml.YAMLRegistered, yaml_tag='!MyTag'):
    """Simple class registered for YAML with a custom tag."""


class Foo(Base):
    """Inherited class, with args."""

    def __init__(self, arg1, arg2, kwarg1='default'):
        """Initialize the instance."""
        super().__init__(arg1, arg2, 4, kwarg2=0)


LOAD_PARAMS = (
    pytest.param(
        'invalid.yml',
        marks=pytest.mark.xfail(reason='Invalid tag',
                                raises=ConstructorError,
                                strict=True)
    ),
    pytest.param(
        'unsafe.yml',
        marks=pytest.mark.xfail(reason='Unsafe command',
                                raises=ConstructorError,
                                strict=True)
    ),
    pytest.param(
        'unregistered.yml',
        marks=pytest.mark.xfail(reason='Class with register_yaml=False',
                                raises=ConstructorError,
                                strict=True)
    ),
    pytest.param(
        'invalid_args.yml',
        marks=pytest.mark.xfail(reason='Class with invalid arguments',
                                raises=TypeError,
                                strict=True)
    ),
    pytest.param('valid.yml')
)


@pytest.mark.parametrize('file_name', LOAD_PARAMS)
def test_loading(datadir: Path, file_name: str) -> None:
    with (datadir / file_name).open() as f:
        syaml.load(f)


@pytest.mark.parametrize('tag', ('!Invalid', 'MyClass'))
def test_invalid_tag(tag: str) -> None:
    """Test that tags are correct.

    Tags for classes inherited from BaseClass should start with "!" and end
    with "Class"
    """
    with pytest.raises(Exception):
        class InvalidClass1(Base, yaml_tag=tag):
            ...
